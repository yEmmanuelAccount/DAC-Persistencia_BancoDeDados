import { randomUUID } from 'node:crypto';
import { RecoveryLog } from './RecoveryLog.js';
import type { ColumnSelection, Operation, PreparedOperation, ReadMode, Row, TableDefinition, TwoPhaseParticipant } from '../types.js';

export type ParticipantName = 'postgres' | 'mongo';

export type OperationInput = Omit<PreparedOperation, 'transactionId'>;
export type TransactionOptions = { order?: ParticipantName[]; failOn?: ParticipantName; parallelPrepare?: boolean };

type ReadSnapshot = {
  tableName: string;
  postgresRows: Row[];
  mongoRows: Row[];
};

export class TwoPhaseCommit {
  private commitBarrier: Promise<void> = Promise.resolve();
  private barrierActive = false;
  private readBlockLogged = false;
  private readModeLogged = false;
  private activeSnapshot: Promise<ReadSnapshot | null> | null = null;

  constructor(
    private readonly postgres: TwoPhaseParticipant,
    private readonly mongo: TwoPhaseParticipant,
    private readonly onEvent: (message: string) => void = () => undefined,
    private readonly readMode: ReadMode = 'blocking',
    private readonly recoveryLog = new RecoveryLog(),
    private readonly parallelPrepare = false,
  ) { }

  run(operation: OperationInput, options: TransactionOptions = {}): Promise<string> {
    return this.runBatch([operation], options);
  }

  async runBatch(operations: OperationInput[], options: TransactionOptions = {}): Promise<string> {
    if (operations.length === 0) throw new Error('Cannot commit an empty transaction');
    const transactionId = randomUUID();
    let releaseBarrier!: () => void;
    const operationBarrier = new Promise<void>((resolve) => { releaseBarrier = resolve; });
    this.commitBarrier = operationBarrier;
    this.barrierActive = true;
    this.readBlockLogged = false;
    this.readModeLogged = false;
    const operationSnapshot = this.readMode === 'mvcc'
      ? this.captureSnapshot(operations[0].table)
      : Promise.resolve(null);
    this.activeSnapshot = operationSnapshot;
    const prepared: TwoPhaseParticipant[] = [];
    const participants: Record<ParticipantName, TwoPhaseParticipant> = { postgres: this.postgres, mongo: this.mongo };
    const order = options.order ?? ['postgres', 'mongo'];
    const event = (message: string): void => this.onEvent(`[2PC ${transactionId}] ${message}`);
    event(`BEGIN operations=${operations.length} table=${operations[0].table.name} order=${order.join(' -> ')} readMode=${this.readMode}`);
    try {
      await operationSnapshot;
      if (this.readMode === 'mvcc') event('READ SNAPSHOT captured before prepare');
      const prepareParticipant = async (name: ParticipantName): Promise<TwoPhaseParticipant> => {
        const participant = participants[name];
        if (!prepared.includes(participant)) prepared.push(participant);
        event(`PREPARE START participant=${name}`);
        if (options.failOn === name) {
          event(`PREPARE FAILED participant=${name}`);
          throw new Error(`Injected failure on ${name} prepare`);
        }
        for (const operation of operations) {
          await participant.prepare({ ...operation, transactionId });
        }
        event(`PREPARE ACK participant=${name}`);
        return participant;
      };
      if (options.parallelPrepare ?? this.parallelPrepare) {
        const results = await Promise.allSettled(order.map(prepareParticipant));
        for (const result of results) {
          if (result.status === 'rejected') throw result.reason;
        }
      } else {
        for (const name of order) await prepareParticipant(name);
      }
      event(`PREPARE COMPLETE participants=${order.join(',')}`);
      this.recoveryLog.append(transactionId, 'PREPARED', order);
      event(`COMMIT START participants=${order.join(',')}`);
      this.recoveryLog.append(transactionId, 'COMMIT_DECISION', order);
      await Promise.all(order.map(async (name) => {
        event(`COMMIT REQUEST participant=${name}`);
        await participants[name].commit(transactionId);
        event(`COMMIT ACK participant=${name}`);
      }));
      event('COMMIT COMPLETE all participants acknowledged');
      this.recoveryLog.append(transactionId, 'COMMITTED', order);
      return transactionId;
    } catch (error) {
      const commitWasDecided = this.recoveryLog.unfinished().some((record) => record.transactionId === transactionId && record.status === 'COMMIT_DECISION');
      if (commitWasDecided) {
        event('COMMIT INCOMPLETE decision remains in recovery log for retry');
      } else {
        event(`ROLLBACK START preparedParticipants=${prepared.length}`);
        await Promise.all(prepared.map(async (participant, index) => {
          const name = order[index];
          await participant.rollback(transactionId);
          event(`ROLLBACK ACK participant=${name}`);
        }));
        this.recoveryLog.append(transactionId, 'ABORTED', order);
        event('ROLLBACK COMPLETE');
      }
      throw error;
    } finally {
      this.barrierActive = false;
      this.activeSnapshot = null;
      releaseBarrier();
      if (this.commitBarrier === operationBarrier) this.commitBarrier = Promise.resolve();
      event('END reads released');
    }
  }

  async recover(): Promise<void> {
    const participants: Record<ParticipantName, TwoPhaseParticipant> = { postgres: this.postgres, mongo: this.mongo };
    for (const record of this.recoveryLog.unfinished()) {
      if (record.status === 'COMMIT_DECISION') {
        await Promise.all(record.participants.map((name) => participants[name as ParticipantName].commit(record.transactionId)));
        this.recoveryLog.append(record.transactionId, 'COMMITTED', record.participants);
      } else {
        await Promise.all(record.participants.map((name) => participants[name as ParticipantName].rollback(record.transactionId)));
        this.recoveryLog.append(record.transactionId, 'ABORTED', record.participants);
      }
    }
    await Promise.all([this.postgres.cleanupOrphanShadows(), this.mongo.cleanupOrphanShadows()]);
  }

  insertPostgresThenMongo(table: TableDefinition, key: string, row: Row, failOn?: 'mongo'): Promise<string> {
    return this.run({ table, key, row, operation: 'insert' }, { order: ['postgres', 'mongo'], failOn });
  }

  insertMongoThenPostgres(table: TableDefinition, key: string, row: Row, failOn?: 'postgres'): Promise<string> {
    return this.run({ table, key, row, operation: 'insert' }, { order: ['mongo', 'postgres'], failOn });
  }

  updateBoth(table: TableDefinition, key: string, row: Row): Promise<string> {
    return this.updatePostgresThenMongo(table, key, row);
  }

  updatePostgresThenMongo(table: TableDefinition, key: string, row: Row, failOn?: 'mongo'): Promise<string> {
    return this.run({ table, key, row, operation: 'update' }, { order: ['postgres', 'mongo'], failOn });
  }

  updateMongoThenPostgres(table: TableDefinition, key: string, row: Row, failOn?: 'postgres'): Promise<string> {
    return this.run({ table, key, row, operation: 'update' }, { order: ['mongo', 'postgres'], failOn });
  }

  deleteBoth(table: TableDefinition, key: string, row: Row): Promise<string> {
    return this.run({ table, key, row, operation: 'delete' });
  }

  async readBoth(table: TableDefinition): Promise<{ postgresRows: Row[]; mongoRows: Row[] }> {
    if (this.barrierActive) {
      if (this.readMode === 'blocking') {
        if (!this.readBlockLogged) {
          this.readBlockLogged = true;
          this.onEvent('READ BLOCKED waiting for 2PC commit or rollback completion');
        }
        await this.commitBarrier;
      } else {
        if (!this.readModeLogged) {
          this.readModeLogged = true;
          this.onEvent('READ MVCC returning snapshot until commit or rollback completes');
        }
        const snapshot = await this.activeSnapshot;
        if (snapshot && snapshot.tableName === table.name) {
          return { postgresRows: snapshot.postgresRows, mongoRows: snapshot.mongoRows };
        }
      }
    }
    const [postgresRows, mongoRows] = await Promise.all([
      this.postgres.readAll(table),
      this.mongo.readAll(table),
    ]);
    return { postgresRows, mongoRows };
  }

  async readBothRow(table: TableDefinition, key: string, columns?: ColumnSelection): Promise<{ postgresRow: Row | null; mongoRow: Row | null }> {
    if (this.barrierActive && this.readMode === 'blocking') await this.commitBarrier;
    if (this.barrierActive && this.readMode === 'mvcc') {
      const snapshot = await this.activeSnapshot;
      if (snapshot && snapshot.tableName === table.name) {
        return {
          postgresRow: this.projectSnapshotRow(snapshot.postgresRows, table, key, columns),
          mongoRow: this.projectSnapshotRow(snapshot.mongoRows, table, key, columns),
        };
      }
    }
    const [postgresRow, mongoRow] = await Promise.all([
      this.postgres.read(table, key, columns),
      this.mongo.read(table, key, columns),
    ]);
    return { postgresRow, mongoRow };
  }

  private async captureSnapshot(table: TableDefinition): Promise<ReadSnapshot> {
    const [postgresRows, mongoRows] = await Promise.all([
      this.postgres.readAll(table),
      this.mongo.readAll(table),
    ]);
    return { tableName: table.name, postgresRows, mongoRows };
  }

  private projectSnapshotRow(rows: Row[], table: TableDefinition, key: string, columns?: ColumnSelection): Row | null {
    const row = rows.find((candidate) => String(candidate[table.keyColumn]) === key);
    if (!row) return null;
    if (!columns) return row;
    return Object.fromEntries(columns.map((column) => [column, row[column] ?? null]));
  }
}
