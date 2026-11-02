import { TwoPhaseCommit, type ParticipantName } from '../twopc/TwoPhaseCommit.js';
import { RecoveryLog } from '../twopc/RecoveryLog.js';
import type { ColumnSelection, ReadMode, Row, TableDefinition, TwoPhaseParticipant } from '../types.js';
import { TwoPhaseTransaction } from './TwoPhaseTransaction.js';
import type { TransactionOptions } from '../twopc/TwoPhaseCommit.js';

type RepositoryOptions = {
  order?: ParticipantName[];
  failOn?: ParticipantName;
};

export abstract class TwoPhaseRepository {
  protected constructor(
    connections: readonly TwoPhaseParticipant[],
    protected readonly table: TableDefinition,
    onEvent: (message: string) => void = () => undefined,
    readMode: ReadMode = 'blocking',
    recoveryLog = new RecoveryLog(),
    parallelPrepare = false,
  ) {
    if (connections.length !== 2) throw new Error('TwoPhaseRepository requires exactly two connections');
    this.coordinator = new TwoPhaseCommit(connections[0], connections[1], onEvent, readMode, recoveryLog, parallelPrepare);
    this.tableReady = Promise.all(connections.map((connection) => connection.createTable(table)))
      .then(() => this.coordinator.recover());
  }

  protected readonly coordinator: TwoPhaseCommit;
  private readonly tableReady: Promise<void>;

  get definition(): TableDefinition {
    return this.table;
  }

  async ready(): Promise<void> {
    await this.tableReady;
  }

  createTransaction(options?: TransactionOptions): TwoPhaseTransaction {
    return new TwoPhaseTransaction(this.coordinator, this.table, options);
  }

  async insert(row: Row, options?: RepositoryOptions): Promise<string> {
    await this.ready();
    return this.coordinator.run({
      table: this.table,
      key: this.keyFromRow(row),
      row,
      operation: 'insert',
    }, options);
  }

  async update(key: string, changes: Row, options?: RepositoryOptions): Promise<string> {
    await this.ready();
    const current = await this.coordinator.readBothRow(this.table, key);
    if (!current.postgresRow || !current.mongoRow) throw new Error(`Cannot update missing ${this.table.name}/${key}`);
    const row = { ...current.postgresRow, ...changes, [this.table.keyColumn]: key };
    return this.coordinator.run({ table: this.table, key, row, operation: 'update' }, options);
  }

  async read(key: string, columns?: ColumnSelection): Promise<{ postgresRow: Row | null; mongoRow: Row | null }> {
    await this.ready();
    return this.coordinator.readBothRow(this.table, key, columns);
  }

  async readAll(): Promise<{ postgresRows: Row[]; mongoRows: Row[] }> {
    await this.ready();
    return this.coordinator.readBoth(this.table);
  }

  async delete(key: string, options?: RepositoryOptions): Promise<string> {
    await this.ready();
    return this.coordinator.run({
      table: this.table,
      key,
      row: { [this.table.keyColumn]: key },
      operation: 'delete',
    }, options);
  }

  private keyFromRow(row: Row): string {
    const key = row[this.table.keyColumn];
    if (key === undefined || key === null) throw new Error(`Missing key column ${this.table.keyColumn}`);
    return String(key);
  }
}

export type { RepositoryOptions };
