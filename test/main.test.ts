import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { RecoveryLog } from '../src/twopc/RecoveryLog.js';
import { TwoPhaseCommit } from '../src/twopc/TwoPhaseCommit.js';
import { TwoPhaseRepository } from '../src/orm/TwoPhaseRepository.js';
import type { PreparedOperation, Row, TableDefinition, TwoPhaseParticipant } from '../src/types.js';

const table: TableDefinition = { name: 'items', keyColumn: 'id', columns: { id: 'text', value: 'integer' } };
const wait = (milliseconds: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, milliseconds));

class FakeParticipant implements TwoPhaseParticipant {
  rows: Row[];
  prepared: PreparedOperation[] = [];
  commits = 0;
  rollbacks = 0;
  constructor(initialRows: Row[] = [], private readonly prepareDelay = 0, private readonly failOnKey?: string) {
    this.rows = initialRows.map((row) => ({ ...row }));
  }
  async initialize(): Promise<void> { }
  async createTable(): Promise<void> { }
  async cleanupOrphanShadows(): Promise<void> { }
  async prepare(operation: PreparedOperation): Promise<void> {
    await wait(this.prepareDelay);
    if (operation.key === this.failOnKey) throw new Error(`prepare failed for ${operation.key}`);
    this.prepared.push(operation);
  }
  async commit(): Promise<void> {
    this.commits += 1;
    for (const operation of this.prepared) {
      const index = this.rows.findIndex((row) => String(row.id) === operation.key);
      if (operation.operation === 'insert') this.rows.push({ ...operation.row });
      if (operation.operation === 'update' && index >= 0) this.rows[index] = { ...operation.row };
      if (operation.operation === 'delete' && index >= 0) this.rows.splice(index, 1);
    }
    this.prepared = [];
  }
  async rollback(): Promise<void> {
    this.rollbacks += 1;
    this.prepared = [];
  }
  async read(_table: TableDefinition, key: string): Promise<Row | null> {
    return this.rows.find((row) => String(row.id) === key) ?? null;
  }
  async readAll(): Promise<Row[]> { return this.rows.map((row) => ({ ...row })); }
  async close(): Promise<void> { }
}

function coordinatorFor(postgres: TwoPhaseParticipant, mongo: TwoPhaseParticipant, mode: 'blocking' | 'mvcc' = 'blocking', parallelPrepare = false): TwoPhaseCommit {
  const directory = mkdtempSync(join(tmpdir(), 'dac-2pc-test-'));
  const log = new RecoveryLog(join(directory, 'recovery.log'));
  return new TwoPhaseCommit(postgres, mongo, undefined, mode, log, parallelPrepare);
}

test('blocking reads wait for both commits', async () => {
  const postgres = new FakeParticipant([{ id: 'one', value: 0 }], 10);
  const mongo = new FakeParticipant([{ id: 'one', value: 0 }], 10);
  const coordinator = coordinatorFor(postgres, mongo);
  const operation = coordinator.run({ table, key: 'one', row: { id: 'one', value: 1 }, operation: 'update' });
  await wait(2);
  const read = coordinator.readBothRow(table, 'one');
  const completedEarly = await Promise.race([read.then(() => true), wait(5).then(() => false)]);
  assert.equal(completedEarly, false);
  await operation;
  assert.deepEqual(await read, { postgresRow: { id: 'one', value: 1 }, mongoRow: { id: 'one', value: 1 } });
});

test('mvcc reads return the old snapshot until commit finishes', async () => {
  const postgres = new FakeParticipant([{ id: 'one', value: 0 }], 10);
  const mongo = new FakeParticipant([{ id: 'one', value: 0 }], 10);
  const coordinator = coordinatorFor(postgres, mongo, 'mvcc');
  const operation = coordinator.run({ table, key: 'one', row: { id: 'one', value: 1 }, operation: 'update' });
  await wait(2);
  assert.deepEqual(await coordinator.readBothRow(table, 'one'), { postgresRow: { id: 'one', value: 0 }, mongoRow: { id: 'one', value: 0 } });
  await operation;
  assert.deepEqual(await coordinator.readBothRow(table, 'one'), { postgresRow: { id: 'one', value: 1 }, mongoRow: { id: 'one', value: 1 } });
});

test('parallel prepare starts both participants concurrently', async () => {
  const postgres = new FakeParticipant([], 40);
  const mongo = new FakeParticipant([], 40);
  const coordinator = coordinatorFor(postgres, mongo, 'blocking', true);
  const started = Date.now();
  await coordinator.run({ table, key: 'one', row: { id: 'one', value: 1 }, operation: 'insert' });
  assert.ok(Date.now() - started < 75);
});

test('transaction provides read-your-writes and commits grouped operations', async () => {
  const postgres = new FakeParticipant([{ id: 'existing', value: 1 }]);
  const mongo = new FakeParticipant([{ id: 'existing', value: 1 }]);
  class ItemsRepository extends TwoPhaseRepository {
    constructor() { super([postgres, mongo], table, undefined, 'blocking', new RecoveryLog(join(mkdtempSync(join(tmpdir(), 'dac-orm-test-')), 'recovery.log'))); }
  }
  const repository = new ItemsRepository();
  await repository.ready();
  const transaction = repository.createTransaction();
  transaction.insert({ id: 'new', value: 2 });
  assert.deepEqual(await transaction.read('new'), { postgresRow: { id: 'new', value: 2 }, mongoRow: { id: 'new', value: 2 } });
  await transaction.update('new', { value: 3 });
  assert.equal((await transaction.read('new')).postgresRow?.value, 3);
  transaction.delete('existing');
  assert.equal((await transaction.read('existing')).postgresRow, null);
  await transaction.commit();
  assert.equal(postgres.commits, 1);
  assert.equal(mongo.commits, 1);
});

test('failed transaction rolls back every prepared participant', async () => {
  const postgres = new FakeParticipant([], 0);
  const mongo = new FakeParticipant([], 0, 'bad');
  const coordinator = coordinatorFor(postgres, mongo);
  await assert.rejects(() => coordinator.runBatch([
    { table, key: 'good', row: { id: 'good', value: 1 }, operation: 'insert' },
    { table, key: 'bad', row: { id: 'bad', value: 2 }, operation: 'insert' },
  ]));
  assert.equal(postgres.rollbacks, 1);
  assert.equal(mongo.rollbacks, 1);
  assert.equal(postgres.rows.length, 0);
});

test('recovery aborts transactions left only in PREPARED state', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'dac-recovery-prepared-'));
  const log = new RecoveryLog(join(directory, 'recovery.log'));
  const operation: PreparedOperation = { table, key: 'prepared', row: { id: 'prepared', value: 1 }, operation: 'insert', transactionId: 'tx-prepared' };
  const postgres = new FakeParticipant();
  const mongo = new FakeParticipant();
  postgres.prepared.push(operation);
  mongo.prepared.push(operation);
  log.append('tx-prepared', 'PREPARED', ['postgres', 'mongo']);
  const coordinator = new TwoPhaseCommit(postgres, mongo, undefined, 'blocking', log);

  await coordinator.recover();

  assert.equal(postgres.rollbacks, 1);
  assert.equal(mongo.rollbacks, 1);
  assert.equal(postgres.commits, 0);
  assert.equal(log.unfinished().length, 0);
  rmSync(directory, { recursive: true, force: true });
});

test('recovery retries transactions with a COMMIT_DECISION', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'dac-recovery-commit-'));
  const log = new RecoveryLog(join(directory, 'recovery.log'));
  const operation: PreparedOperation = { table, key: 'committed', row: { id: 'committed', value: 2 }, operation: 'insert', transactionId: 'tx-commit' };
  const postgres = new FakeParticipant();
  const mongo = new FakeParticipant();
  postgres.prepared.push(operation);
  mongo.prepared.push(operation);
  log.append('tx-commit', 'PREPARED', ['postgres', 'mongo']);
  log.append('tx-commit', 'COMMIT_DECISION', ['postgres', 'mongo']);
  const coordinator = new TwoPhaseCommit(postgres, mongo, undefined, 'blocking', log);

  await coordinator.recover();

  assert.equal(postgres.commits, 1);
  assert.equal(mongo.commits, 1);
  assert.deepEqual(await postgres.read(table, 'committed'), { id: 'committed', value: 2 });
  assert.deepEqual(await mongo.read(table, 'committed'), { id: 'committed', value: 2 });
  assert.equal(log.unfinished().length, 0);
  rmSync(directory, { recursive: true, force: true });
});
