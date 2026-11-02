import { randomUUID } from 'node:crypto';
import type { Operation, PreparedOperation, Row, TableDefinition, TwoPhaseParticipant } from '../types.js';

type ParticipantName = 'postgres' | 'mongo';

type OperationInput = Omit<PreparedOperation, 'transactionId'>;

export class TwoPhaseCommit {
  private commitBarrier: Promise<void> = Promise.resolve();

  constructor(private readonly postgres: TwoPhaseParticipant, private readonly mongo: TwoPhaseParticipant) { }

  async run(operation: OperationInput, options: { order?: ParticipantName[]; failOn?: ParticipantName } = {}): Promise<string> {
    const transactionId = randomUUID();
    let releaseBarrier!: () => void;
    const operationBarrier = new Promise<void>((resolve) => { releaseBarrier = resolve; });
    this.commitBarrier = operationBarrier;
    const prepared: TwoPhaseParticipant[] = [];
    const participants: Record<ParticipantName, TwoPhaseParticipant> = { postgres: this.postgres, mongo: this.mongo };
    try {
      for (const name of options.order ?? ['postgres', 'mongo']) {
        const participant = participants[name];
        if (options.failOn === name) throw new Error(`Injected failure on ${name} prepare`);
        await participant.prepare({ ...operation, transactionId });
        prepared.push(participant);
      }
      await Promise.all(prepared.map((participant) => participant.commit(transactionId)));
      return transactionId;
    } catch (error) {
      await Promise.allSettled(prepared.map((participant) => participant.rollback(transactionId)));
      throw error;
    } finally {
      releaseBarrier();
      if (this.commitBarrier === operationBarrier) this.commitBarrier = Promise.resolve();
    }
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
    await this.commitBarrier;
    const [postgresRows, mongoRows] = await Promise.all([
      this.postgres.readAll(table),
      this.mongo.readAll(table),
    ]);
    return { postgresRows, mongoRows };
  }
}
