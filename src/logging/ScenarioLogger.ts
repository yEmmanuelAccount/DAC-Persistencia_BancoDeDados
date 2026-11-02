import { appendFileSync, writeFileSync } from 'node:fs';
import type { Row } from '../types.js';

type Cell = string | number | boolean | null;

type RowSnapshot = {
  database: string;
  row: Row | null;
};

type DatabaseSnapshot = {
  database: string;
  rows: Row[];
};

const stringifyCell = (value: Cell): string => value === null ? 'null' : String(value);

export class ScenarioLogger {
  private readonly lines: string[] = [];

  constructor(private readonly filePath = '2pc-report.txt') {
    writeFileSync(this.filePath, '', 'utf8');
    this.info(`2PC scenario report - ${new Date().toISOString()}`);
  }

  info(message: string): void {
    const line = `[${new Date().toISOString()}] ${message}`;
    this.lines.push(line);
    console.log(line);
  }

  operation(operation: string, status: string, key: string, detail = ''): void {
    this.info(`${operation.toUpperCase()} ${status.toUpperCase()} | key=${key}${detail ? ` | ${detail}` : ''}`);
  }

  rows(title: string, snapshots: RowSnapshot[]): void {
    this.info(title);
    const columns = ['database', 'row'];
    const values = snapshots.map(({ database, row }) => [database, row ? JSON.stringify(row) : 'null']);
    this.table(columns, values);
  }

  database(title: string, snapshots: DatabaseSnapshot[]): void {
    this.info(title);
    const columnNames = [...new Set(snapshots.flatMap(({ rows }) => rows.flatMap((row) => Object.keys(row))))];
    const columns = ['database', ...columnNames];
    const values: Cell[][] = [];
    for (const snapshot of snapshots) {
      if (snapshot.rows.length === 0) {
        values.push([snapshot.database, ...columnNames.map(() => 'empty')]);
        continue;
      }
      for (const row of snapshot.rows) {
        values.push([snapshot.database, ...columnNames.map((column) => row[column] ?? null)]);
      }
    }
    this.table(columns, values);
  }

  table(columns: string[], values: Cell[][]): void {
    const widths = columns.map((column, index) => Math.max(
      column.length,
      ...values.map((row) => stringifyCell(row[index] ?? null).length),
    ));
    const separator = `+${widths.map((width) => '-'.repeat(width + 2)).join('+')}+`;
    const formatRow = (row: Cell[]): string => `|${row.map((cell, index) => ` ${stringifyCell(cell).padEnd(widths[index])} `).join('|')}|`;
    const tableLines = [separator, formatRow(columns), separator, ...values.map(formatRow), separator];
    this.lines.push(...tableLines);
    console.log(tableLines.join('\n'));
  }

  flush(): void {
    appendFileSync(this.filePath, `${this.lines.join('\n')}\n`, 'utf8');
    this.lines.length = 0;
  }
}
