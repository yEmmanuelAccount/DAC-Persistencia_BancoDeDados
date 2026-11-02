import 'dotenv/config';
import { MongoConnection } from './connection/MongoConnection.js';
import { PostgresConnection } from './connection/PostgresConnection.js';
import { ScenarioLogger } from './logging/ScenarioLogger.js';
import { TwoPhaseCommit } from './twopc/TwoPhaseCommit.js';
import type { Row, TableDefinition } from './types.js';

const postgres = new PostgresConnection();
const mongo = new MongoConnection();
const coordinator = new TwoPhaseCommit(postgres, mongo);
const logger = new ScenarioLogger(process.env.REPORT_FILE ?? '2pc-report.txt');

async function logDatabase(table: TableDefinition, title: string): Promise<void> {
  const { postgresRows, mongoRows } = await coordinator.readBoth(table);
  logger.database(title, [
    { database: 'PostgreSQL', rows: postgresRows },
    { database: 'MongoDB', rows: mongoRows },
  ]);
}

function comparableRows(table: TableDefinition, rows: Row[]): Row[] {
  return rows
    .map((row) => Object.fromEntries(Object.entries(row).map(([column, value]) => {
      const type = table.columns[column];
      if ((type === 'integer' || type === 'decimal') && typeof value === 'string') return [column, Number(value)];
      return [column, value];
    })) as Row)
    .sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));
}

async function pollBoth(table: TableDefinition, key: string, durationMs: number): Promise<void> {
  const intervalMs = Number(process.env.POLL_INTERVAL_MS ?? 10);
  const end = Date.now() + durationMs;
  let reads = 0;
  let mismatches = 0;
  let mismatchLogged = false;
  logger.info(`Polling iniciado: ${intervalMs}ms por ${durationMs}ms para ${key}`);
  while (Date.now() < end) {
    const { postgresRows, mongoRows } = await coordinator.readBoth(table);
    reads += 1;
    if (JSON.stringify(comparableRows(table, postgresRows)) !== JSON.stringify(comparableRows(table, mongoRows))) {
      mismatches += 1;
      if (!mismatchLogged) {
        mismatchLogged = true;
        logger.info(`Divergencia durante polling de ${key}`);
        logger.database('Banco completo durante a divergencia', [
          { database: 'PostgreSQL', rows: postgresRows },
          { database: 'MongoDB', rows: mongoRows },
        ]);
      }
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  logger.info(`Polling finalizado: ${reads} leituras, ${mismatches} divergencias`);
}

async function insertAndLog(table: TableDefinition, key: string, row: Row): Promise<void> {
  await coordinator.insertPostgresThenMongo(table, key, row);
  logger.operation('insert', 'committed', key);
  await logDatabase(table, `Banco completo depois do INSERT ${key}`);
}

async function main(): Promise<void> {
  await postgres.initialize();
  await mongo.initialize();

  const inventory: TableDefinition = {
    name: 'inventory',
    keyColumn: 'sku',
    columns: {
      // Stock Keeping Unit
      sku: 'text',
      description: 'text',
      quantity: 'integer',
      price: 'decimal',
      active: 'boolean',
    },
  };
  await postgres.createTable(inventory);
  await mongo.createTable(inventory);
  logger.info('Tabela/colecao inventory criada com lock automatico nos dois bancos');

  const products: Row[] = [
    { sku: 'keyboard-01', description: 'Mechanical keyboard', quantity: 20, price: 249.9, active: true },
    { sku: 'mouse-01', description: 'Wireless mouse', quantity: 35, price: 99.9, active: true },
    { sku: 'monitor-01', description: '27 inch monitor', quantity: 12, price: 1299.9, active: true },
  ];
  for (const product of products) await insertAndLog(inventory, String(product.sku), product);

  const failedMongoKey = 'headset-01';
  await logDatabase(inventory, 'Banco completo ANTES da falha no MongoDB');
  try {
    await coordinator.insertPostgresThenMongo(inventory, failedMongoKey, { sku: failedMongoKey, description: 'Wireless headset', quantity: 18, price: 179.9, active: true }, 'mongo');
  } catch (error) {
    logger.operation('insert', 'failed and rolled back', failedMongoKey, (error as Error).message);
  }
  await logDatabase(inventory, 'Banco completo DEPOIS da falha no MongoDB');

  const failedPostgresKey = 'webcam-01';
  await logDatabase(inventory, 'Banco completo ANTES da falha no PostgreSQL');
  try {
    await coordinator.insertMongoThenPostgres(inventory, failedPostgresKey, { sku: failedPostgresKey, description: 'Full HD webcam', quantity: 16, price: 219.9, active: true }, 'postgres');
  } catch (error) {
    logger.operation('insert', 'failed and rolled back', failedPostgresKey, (error as Error).message);
  }
  await logDatabase(inventory, 'Banco completo DEPOIS da falha no PostgreSQL');

  const failedKeyboardUpdate: Row = { ...products[0], description: 'Mechanical keyboard - stock adjustment', quantity: 19, price: 244.9 };
  await logDatabase(inventory, 'Banco completo ANTES da falha de update no MongoDB');
  try {
    await coordinator.updatePostgresThenMongo(inventory, 'keyboard-01', failedKeyboardUpdate, 'mongo');
  } catch (error) {
    logger.operation('update', 'failed and rolled back', 'keyboard-01', (error as Error).message);
  }
  await logDatabase(inventory, 'Banco completo DEPOIS da falha de update no MongoDB');

  const failedMouseUpdate: Row = { ...products[1], description: 'Wireless mouse - stock adjustment', quantity: 33, price: 94.9 };
  await logDatabase(inventory, 'Banco completo ANTES da falha de update no PostgreSQL');
  try {
    await coordinator.updateMongoThenPostgres(inventory, 'mouse-01', failedMouseUpdate, 'postgres');
  } catch (error) {
    logger.operation('update', 'failed and rolled back', 'mouse-01', (error as Error).message);
  }
  await logDatabase(inventory, 'Banco completo DEPOIS da falha de update no PostgreSQL');

  const updates: Row[] = [
    { ...products[0], description: 'Mechanical keyboard - updated', quantity: 15, price: 259.9 },
    { ...products[1], description: 'Wireless mouse - updated', quantity: 28, price: 109.9 },
    { ...products[2], description: '27 inch monitor - updated', quantity: 10, price: 1249.9 },
  ];
  for (const update of updates) {
    const key = String(update.sku);
    const watcher = pollBoth(inventory, key, 250);
    await coordinator.updateBoth(inventory, key, update);
    await watcher;
    logger.operation('update', 'committed', key);
    await logDatabase(inventory, `Banco completo depois do UPDATE ${key}`);
  }

  const deleteKey = 'to-delete';
  await insertAndLog(inventory, deleteKey, { ...products[0], sku: deleteKey });
  await coordinator.deleteBoth(inventory, deleteKey, { ...products[0], sku: deleteKey });
  logger.operation('delete', 'committed', deleteKey);
  await logDatabase(inventory, 'Banco completo depois do DELETE');
}

main()
  .catch((error) => {
    logger.info(`ERRO FATAL: ${(error as Error).message}`);
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await Promise.allSettled([postgres.close(), mongo.close()]);
    logger.flush();
    logger.info(`Relatorio salvo em ${process.env.REPORT_FILE ?? '2pc-report.txt'}`);
    logger.flush();
  });
