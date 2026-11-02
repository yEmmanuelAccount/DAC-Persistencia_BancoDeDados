import { TwoPhaseCommit, type OperationInput, type TransactionOptions } from '../twopc/TwoPhaseCommit.js';
import type { ColumnSelection, Row, TableDefinition } from '../types.js';

export class TwoPhaseTransaction {
  private readonly operations: OperationInput[] = [];
  private committed = false;

  constructor(
    private readonly coordinator: TwoPhaseCommit,
    private readonly table: TableDefinition,
    private readonly options: TransactionOptions = {},
  ) { }

  insert(row: Row): this {
    this.ensureOpen();
    this.operations.push({ table: this.table, key: this.keyFromRow(row), row, operation: 'insert' });
    return this;
  }

  async update(key: string, changes: Row): Promise<this> {
    this.ensureOpen();
    const current = await this.read(key);
    if (!current.postgresRow || !current.mongoRow) throw new Error(`Cannot update missing ${this.table.name}/${key}`);
    this.operations.push({
      table: this.table,
      key,
      row: { ...current.postgresRow, ...changes, [this.table.keyColumn]: key },
      operation: 'update',
    });
    return this;
  }

  read(key: string, columns?: ColumnSelection): Promise<{ postgresRow: Row | null; mongoRow: Row | null }> {
    this.ensureOpen();
    const pending = this.pendingRow(key);
    if (pending) {
      const row = pending.operation === 'delete' ? null : this.projectRow(pending.row, columns);
      return Promise.resolve({ postgresRow: row, mongoRow: row });
    }
    return this.coordinator.readBothRow(this.table, key, columns);
  }

  delete(key: string): this {
    this.ensureOpen();
    this.operations.push({
      table: this.table,
      key,
      row: { [this.table.keyColumn]: key },
      operation: 'delete',
    });
    return this;
  }

  async commit(): Promise<string> {
    this.ensureOpen();
    const transactionId = await this.coordinator.runBatch(this.operations, this.options);
    this.committed = true;
    return transactionId;
  }

  private ensureOpen(): void {
    if (this.committed) throw new Error('Transaction has already been committed');
  }

  private pendingRow(key: string): OperationInput | null {
    return [...this.operations].reverse().find((operation) => operation.key === key) ?? null;
  }

  private projectRow(row: Row, columns?: ColumnSelection): Row {
    if (!columns) return row;
    return Object.fromEntries(columns.map((column) => [column, row[column] ?? null]));
  }

  private keyFromRow(row: Row): string {
    const key = row[this.table.keyColumn];
    if (key === undefined || key === null) throw new Error(`Missing key column ${this.table.keyColumn}`);
    return String(key);
  }
}
