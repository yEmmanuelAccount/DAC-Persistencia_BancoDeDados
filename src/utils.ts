import { ScenarioLogger } from './logging/ScenarioLogger.js';
import { TwoPhaseRepository } from './orm/TwoPhaseRepository.js';
import type { Row, TableDefinition } from './types.js';

export async function logDatabase(repository: TwoPhaseRepository, logger: ScenarioLogger, title: string): Promise<void> {
  const { postgresRows, mongoRows } = await repository.readAll();
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

export async function pollBoth(repository: TwoPhaseRepository, logger: ScenarioLogger, key: string, durationMs: number): Promise<void> {
  const table = repository.definition;
  const intervalMs = Number(process.env.POLL_INTERVAL_MS ?? 10);
  const end = Date.now() + durationMs;
  let reads = 0;
  let mismatches = 0;
  let mismatchLogged = false;
  logger.info(`Polling iniciado: ${intervalMs}ms por ${durationMs}ms para ${key}`);
  while (Date.now() < end) {
    const { postgresRows, mongoRows } = await repository.readAll();
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

export async function insertAndLog(repository: TwoPhaseRepository, logger: ScenarioLogger, key: string, row: Row): Promise<void> {
  await repository.insert(row);
  logger.operation('insert', 'committed', key);
  await logDatabase(repository, logger, `Banco completo depois do INSERT ${key}`);
}

export async function runExpectedFailure(
  repository: TwoPhaseRepository,
  logger: ScenarioLogger,
  operation: 'insert' | 'update',
  key: string,
  failureDescription: string,
  action: () => Promise<string>,
): Promise<void> {
  await logDatabase(repository, logger, `Banco completo ANTES de ${failureDescription}`);
  try {
    await action();
    logger.operation(operation, 'unexpectedly committed', key, 'failure injection did not fail');
  } catch (error) {
    logger.operation(operation, 'failed and rolled back', key, (error as Error).message);
  }
  await logDatabase(repository, logger, `Banco completo DEPOIS de ${failureDescription}`);
}
