import { createHash } from 'node:crypto';
import { Pool } from 'pg';
import type { ColumnSelection, PreparedOperation, Row, TableDefinition, TwoPhaseParticipant } from '../types.js';

const SHADOW_PREFIX = 'twopc_shadow_';
const LOCK_COLUMN = '__twopc_lock_tx_id';

const quoteIdentifier = (identifier: string): string => {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(identifier)) throw new Error(`Invalid SQL identifier: ${identifier}`);
  return `"${identifier}"`;
};

const shadowName = (transactionId: string): string =>
  `${SHADOW_PREFIX}${createHash('sha256').update(transactionId).digest('hex').slice(0, 24)}`;

const sqlType = (type: TableDefinition['columns'][string]): string => ({
  text: 'TEXT',
  integer: 'INTEGER',
  decimal: 'NUMERIC',
  boolean: 'BOOLEAN',
}[type]);

const selectedColumns = (table: TableDefinition, columns?: ColumnSelection): string[] => {
  const selected = columns ? [...columns] : Object.keys(table.columns);
  for (const column of selected) {
    if (!table.columns[column]) throw new Error(`Unknown column ${column} on ${table.name}`);
  }
  return selected;
};

export class PostgresConnection implements TwoPhaseParticipant {
  private readonly pool: Pool;

  constructor(connectionString = process.env.POSTGRES_URL) {
    this.pool = new Pool({ connectionString });
  }

  async initialize(): Promise<void> {
    await this.pool.query('SELECT 1');
  }

  async cleanupOrphanShadows(): Promise<void> {
    const result = await this.pool.query(
      'SELECT tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE $1',
      [`${SHADOW_PREFIX}%`],
    );
    for (const row of result.rows) await this.pool.query(`DROP TABLE IF EXISTS ${quoteIdentifier(row.tablename)}`);
    const tables = await this.pool.query(
      'SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = current_schema() AND column_name = $1 AND table_name NOT LIKE $2',
      [LOCK_COLUMN, `${SHADOW_PREFIX}%`],
    );
    for (const table of tables.rows) {
      await this.pool.query(`UPDATE ${quoteIdentifier(table.table_name)} SET ${quoteIdentifier(LOCK_COLUMN)} = NULL`);
    }
  }

  async createTable(table: TableDefinition): Promise<void> {
    if (!table.columns[table.keyColumn]) throw new Error(`Key column ${table.keyColumn} is missing from ${table.name}`);
    const columns = Object.entries(table.columns).map(([name, type]) => `${quoteIdentifier(name)} ${sqlType(type)} NOT NULL`);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${quoteIdentifier(table.name)} (
        ${columns.join(', ')},
        ${quoteIdentifier(LOCK_COLUMN)} TEXT,
        PRIMARY KEY (${quoteIdentifier(table.keyColumn)})
      )
    `);
    await this.pool.query(`ALTER TABLE ${quoteIdentifier(table.name)} ADD COLUMN IF NOT EXISTS ${quoteIdentifier(LOCK_COLUMN)} TEXT`);
  }

  private async createShadowTable(operation: PreparedOperation): Promise<string> {
    const name = shadowName(operation.transactionId);
    const columns = Object.entries(operation.table.columns).map(([column, type]) => `${quoteIdentifier(column)} ${sqlType(type)}`);
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${quoteIdentifier(name)} (
        "__twopc_table" TEXT NOT NULL,
        "__twopc_key_column" TEXT NOT NULL,
        "__twopc_operation" TEXT NOT NULL,
        "__twopc_key" TEXT NOT NULL,
        ${columns.join(', ')},
        PRIMARY KEY ("__twopc_key")
      )
    `);
    return name;
  }

  async prepare(operation: PreparedOperation): Promise<void> {
    const { transactionId, table, operation: kind, key, row } = operation;
    await this.createTable(table);
    const tableName = quoteIdentifier(table.name);
    if (kind === 'insert') {
      const existing = await this.pool.query(`SELECT 1 FROM ${tableName} WHERE ${quoteIdentifier(table.keyColumn)} = $1`, [key]);
      if (existing.rowCount) throw new Error(`Postgres: ${table.name}/${key} already exists`);
    } else {
      const locked = await this.pool.query(
        `UPDATE ${tableName} SET ${quoteIdentifier(LOCK_COLUMN)} = $1 WHERE ${quoteIdentifier(table.keyColumn)} = $2 AND (${quoteIdentifier(LOCK_COLUMN)} IS NULL OR ${quoteIdentifier(LOCK_COLUMN)} = $1) RETURNING 1`,
        [transactionId, key],
      );
      if (!locked.rowCount) throw new Error(`Postgres: ${table.name}/${key} does not exist or is locked`);
    }
    const shadow = await this.createShadowTable(operation);
    const columns = Object.keys(table.columns);
    const metadata = [table.name, table.keyColumn, kind, key];
    const values = columns.map((column) => row[column] ?? null);
    const placeholders = [...metadata, ...values].map((_, index) => `$${index + 1}`).join(', ');
    await this.pool.query(
      `INSERT INTO ${quoteIdentifier(shadow)} ("__twopc_table", "__twopc_key_column", "__twopc_operation", "__twopc_key", ${columns.map(quoteIdentifier).join(', ')}) VALUES (${placeholders})`,
      [...metadata, ...values],
    );
  }

  async commit(transactionId: string): Promise<void> {
    const shadow = shadowName(transactionId);
    const result = await this.pool.query(`SELECT * FROM ${quoteIdentifier(shadow)}`);
    for (const staged of result.rows) {
      const table = staged.__twopc_table as string;
      const keyColumn = staged.__twopc_key_column as string;
      const columns = Object.keys(staged).filter((column) => !column.startsWith('__twopc_'));
      const values = columns.map((column) => staged[column]);
      if (staged.__twopc_operation === 'insert') {
        const updateColumns = columns.filter((column) => column !== keyColumn);
        const conflictUpdates = updateColumns.map((column) => `${quoteIdentifier(column)} = EXCLUDED.${quoteIdentifier(column)}`);
        await this.pool.query(
          `INSERT INTO ${quoteIdentifier(table)} (${columns.map(quoteIdentifier).join(', ')}, ${quoteIdentifier(LOCK_COLUMN)}) VALUES (${values.map((_, index) => `$${index + 1}`).join(', ')}, NULL) ON CONFLICT (${quoteIdentifier(keyColumn)}) DO UPDATE SET ${[...conflictUpdates, `${quoteIdentifier(LOCK_COLUMN)} = NULL`].join(', ')}`,
          values,
        );
      } else if (staged.__twopc_operation === 'update') {
        const updateColumns = columns.filter((column) => column !== keyColumn);
        const updates = updateColumns.map((column, index) => `${quoteIdentifier(column)} = $${index + 1}`);
        await this.pool.query(
          `UPDATE ${quoteIdentifier(table)} SET ${updates.join(', ')}, ${quoteIdentifier(LOCK_COLUMN)} = NULL WHERE ${quoteIdentifier(keyColumn)} = $${updates.length + 1}`,
          [...updateColumns.map((column) => staged[column]), staged[keyColumn]],
        );
      } else {
        await this.pool.query(
          `DELETE FROM ${quoteIdentifier(table)} WHERE ${quoteIdentifier(keyColumn)} = $1`,
          [staged[keyColumn]],
        );
      }
    }
    await this.pool.query(`DROP TABLE IF EXISTS ${quoteIdentifier(shadow)}`);
  }

  async rollback(transactionId: string): Promise<void> {
    const shadow = shadowName(transactionId);
    const tables = await this.pool.query(
      'SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = current_schema() AND column_name = $1 AND table_name NOT LIKE $2',
      [LOCK_COLUMN, `${SHADOW_PREFIX}%`],
    );
    for (const table of tables.rows) {
      await this.pool.query(`UPDATE ${quoteIdentifier(table.table_name)} SET ${quoteIdentifier(LOCK_COLUMN)} = NULL WHERE ${quoteIdentifier(LOCK_COLUMN)} = $1`, [transactionId]);
    }
    await this.pool.query(`DROP TABLE IF EXISTS ${quoteIdentifier(shadow)}`);
  }

  async read(table: TableDefinition, key: string, requestedColumns?: ColumnSelection): Promise<Row | null> {
    const columns = selectedColumns(table, requestedColumns).map(quoteIdentifier).join(', ');
    const result = await this.pool.query(`SELECT ${columns} FROM ${quoteIdentifier(table.name)} WHERE ${quoteIdentifier(table.keyColumn)} = $1`, [key]);
    return result.rows[0] ?? null;
  }

  async readAll(table: TableDefinition): Promise<Row[]> {
    const columns = Object.keys(table.columns).map(quoteIdentifier).join(', ');
    const result = await this.pool.query(`SELECT ${columns} FROM ${quoteIdentifier(table.name)} ORDER BY ${quoteIdentifier(table.keyColumn)}`);
    return result.rows;
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
