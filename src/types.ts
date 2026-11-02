export type ColumnType = 'text' | 'integer' | 'decimal' | 'boolean';

export type TableDefinition = {
  name: string;
  keyColumn: string;
  columns: Record<string, ColumnType>;
};

export type Row = Record<string, string | number | boolean | null>;

export type Operation = 'insert' | 'update' | 'delete';

export type PreparedOperation = {
  transactionId: string;
  table: TableDefinition;
  operation: Operation;
  key: string;
  row: Row;
};

export interface TwoPhaseParticipant {
  initialize(): Promise<void>;
  createTable(table: TableDefinition): Promise<void>;
  prepare(operation: PreparedOperation): Promise<void>;
  commit(transactionId: string): Promise<void>;
  rollback(transactionId: string): Promise<void>;
  read(table: TableDefinition, key: string): Promise<Row | null>;
  readAll(table: TableDefinition): Promise<Row[]>;
  close(): Promise<void>;
}
