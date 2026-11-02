import 'dotenv/config';
import { MongoConnection } from './connection/MongoConnection.js';
import { PostgresConnection } from './connection/PostgresConnection.js';
import { ScenarioLogger } from './logging/ScenarioLogger.js';
import { Inventory } from './orm/Inventory.js';
import { insertAndLog, logDatabase, pollBoth, runExpectedFailure } from './utils.js';
import type { ReadMode, Row } from './types.js';

const postgres = new PostgresConnection();
const mongo = new MongoConnection();
const logger = new ScenarioLogger(process.env.REPORT_FILE ?? '2pc-report.txt');
const READ_MODE: ReadMode = process.env.READ_MODE === 'mvcc' ? 'mvcc' : 'blocking';
const PARALLEL_PREPARE = process.env.PARALLEL_PREPARE === 'true';

async function main(): Promise<void> {
  await postgres.initialize();
  await mongo.initialize();
  logger.info(`Read consistency mode: ${READ_MODE}`);

  const inventory = Inventory.getInstance([postgres, mongo], (message) => logger.info(message), READ_MODE, PARALLEL_PREPARE);
  await inventory.ready();
  logger.info('Tabela/colecao inventory criada com lock automatico nos dois bancos');

  const products: Row[] = [
    { sku: 'keyboard-01', description: 'Mechanical keyboard', quantity: 20, price: 249.9, active: true },
    { sku: 'mouse-01', description: 'Wireless mouse', quantity: 35, price: 99.9, active: true },
    { sku: 'monitor-01', description: '27 inch monitor', quantity: 12, price: 1299.9, active: true },
  ];
  for (const product of products) await insertAndLog(inventory, logger, String(product.sku), product);

  const failedMongoKey = 'headset-01';
  await runExpectedFailure(inventory, logger, 'insert', failedMongoKey, 'falha no MongoDB', () => inventory.insert(
    { sku: failedMongoKey, description: 'Wireless headset', quantity: 18, price: 179.9, active: true },
    { order: ['postgres', 'mongo'], failOn: 'mongo' },
  ));

  const failedPostgresKey = 'webcam-01';
  await runExpectedFailure(inventory, logger, 'insert', failedPostgresKey, 'falha no PostgreSQL', () => inventory.insert(
    { sku: failedPostgresKey, description: 'Full HD webcam', quantity: 16, price: 219.9, active: true },
    { order: ['mongo', 'postgres'], failOn: 'postgres' },
  ));

  const failedKeyboardUpdate: Row = { ...products[0], description: 'Mechanical keyboard - stock adjustment', quantity: 19, price: 244.9 };
  await runExpectedFailure(inventory, logger, 'update', 'keyboard-01', 'falha de update no MongoDB', () => inventory.update(
    'keyboard-01',
    failedKeyboardUpdate,
    { order: ['postgres', 'mongo'], failOn: 'mongo' },
  ));

  const failedMouseUpdate: Row = { ...products[1], description: 'Wireless mouse - stock adjustment', quantity: 33, price: 94.9 };
  await runExpectedFailure(inventory, logger, 'update', 'mouse-01', 'falha de update no PostgreSQL', () => inventory.update(
    'mouse-01',
    failedMouseUpdate,
    { order: ['mongo', 'postgres'], failOn: 'postgres' },
  ));

  const updates: Row[] = [
    { ...products[0], description: 'Mechanical keyboard - updated', quantity: 15, price: 259.9 },
    { ...products[1], description: 'Wireless mouse - updated', quantity: 28, price: 109.9 },
    { ...products[2], description: '27 inch monitor - updated', quantity: 10, price: 1249.9 },
  ];
  for (const update of updates) {
    const key = String(update.sku);
    const watcher = pollBoth(inventory, logger, key, 250);
    try {
      await inventory.update(key, update);
    } finally {
      await watcher;
    }
    logger.operation('update', 'committed', key);
    await logDatabase(inventory, logger, `Banco completo depois do UPDATE ${key}`);
  }

  const deleteKey = 'to-delete';
  await insertAndLog(inventory, logger, deleteKey, { ...products[0], sku: deleteKey });
  await inventory.delete(deleteKey);
  logger.operation('delete', 'committed', deleteKey);
  await logDatabase(inventory, logger, 'Banco completo depois do DELETE');
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
