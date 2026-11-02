import { createHash } from 'node:crypto';
import { MongoClient, type Collection, type Db } from 'mongodb';
import type { PreparedOperation, Row, TableDefinition, TwoPhaseParticipant } from '../types.js';

type MongoRow = Row & { __twopc_lock_tx_id?: string | null };

type ShadowRecord = {
  _id: string;
  table: string;
  keyColumn: string;
  operation: PreparedOperation['operation'];
  key: string;
  row: Row;
};

const SHADOW_PREFIX = 'twopc_shadow_';
const LOCK_FIELD = '__twopc_lock_tx_id';

const shadowName = (transactionId: string): string =>
  `${SHADOW_PREFIX}${createHash('sha256').update(transactionId).digest('hex').slice(0, 24)}`;

export class MongoConnection implements TwoPhaseParticipant {
  private readonly client: MongoClient;
  private database!: Db;

  constructor(url = process.env.MONGO_URL ?? 'mongodb://localhost:27017', database = process.env.MONGO_DATABASE ?? 'dac') {
    this.client = new MongoClient(url);
    this.databaseName = database;
  }

  private readonly databaseName: string;

  async initialize(): Promise<void> {
    await this.client.connect();
    this.database = this.client.db(this.databaseName);
    const collections = await this.database.listCollections({ name: { $regex: `^${SHADOW_PREFIX}` } }).toArray();
    for (const collection of collections) await this.database.collection(collection.name).drop();
    const dataCollections = await this.database.listCollections({ name: { $not: { $regex: `^${SHADOW_PREFIX}` } } }).toArray();
    for (const collection of dataCollections) {
      await this.database.collection(collection.name).updateMany({ [LOCK_FIELD]: { $exists: true } }, { $unset: { [LOCK_FIELD]: '' } });
    }
  }

  async createTable(table: TableDefinition): Promise<void> {
    if (!table.columns[table.keyColumn]) throw new Error(`Key column ${table.keyColumn} is missing from ${table.name}`);
    try {
      await this.database.createCollection(table.name);
    } catch (error) {
      if ((error as { code?: number }).code !== 48) throw error;
    }
    await this.database.collection<MongoRow>(table.name).createIndex({ [table.keyColumn]: 1 }, { unique: true });
  }

  private shadow(transactionId: string): Collection<ShadowRecord> {
    return this.database.collection<ShadowRecord>(shadowName(transactionId));
  }

  async prepare(operation: PreparedOperation): Promise<void> {
    const { transactionId, table, operation: kind, key, row } = operation;
    await this.createTable(table);
    const collection = this.database.collection<MongoRow>(table.name);
    if (kind === 'insert') {
      if (await collection.findOne({ [table.keyColumn]: key })) throw new Error(`MongoDB: ${table.name}/${key} already exists`);
    } else {
      const locked = await collection.findOneAndUpdate(
        { [table.keyColumn]: key, $or: [{ [LOCK_FIELD]: null }, { [LOCK_FIELD]: { $exists: false } }, { [LOCK_FIELD]: transactionId }] },
        { $set: { [LOCK_FIELD]: transactionId } },
      );
      if (!locked) throw new Error(`MongoDB: ${table.name}/${key} does not exist or is locked`);
    }
    await this.shadow(transactionId).insertOne({ _id: `${table.name}:${key}`, table: table.name, keyColumn: table.keyColumn, operation: kind, key, row });
  }

  async commit(transactionId: string): Promise<void> {
    const records = await this.shadow(transactionId).find().toArray();
    for (const record of records) {
      const collection = this.database.collection<MongoRow>(record.table);
      if (record.operation === 'insert') {
        await collection.insertOne({ ...record.row });
      } else if (record.operation === 'update') {
        await collection.replaceOne({ [record.keyColumn]: record.key, [LOCK_FIELD]: transactionId }, { ...record.row });
      } else {
        await collection.deleteOne({ [record.keyColumn]: record.key, [LOCK_FIELD]: transactionId });
      }
    }
    await this.database.collection(shadowName(transactionId)).drop().catch(() => undefined);
  }

  async rollback(transactionId: string): Promise<void> {
    const collections = await this.database.listCollections({ name: { $not: { $regex: `^${SHADOW_PREFIX}` } } }).toArray();
    for (const collection of collections) {
      await this.database.collection<MongoRow>(collection.name).updateMany({ [LOCK_FIELD]: transactionId }, { $unset: { [LOCK_FIELD]: '' } });
    }
    await this.database.collection(shadowName(transactionId)).drop().catch(() => undefined);
  }

  async read(table: TableDefinition, key: string): Promise<Row | null> {
    const row = await this.database.collection<MongoRow>(table.name).findOne({ [table.keyColumn]: key }, { projection: { [LOCK_FIELD]: 0, _id: 0 } });
    return row;
  }

  async readAll(table: TableDefinition): Promise<Row[]> {
    return this.database.collection<MongoRow>(table.name)
      .find({}, { projection: { [LOCK_FIELD]: 0, _id: 0 } })
      .sort({ [table.keyColumn]: 1 })
      .toArray();
  }

  async close(): Promise<void> {
    await this.client.close();
  }
}
