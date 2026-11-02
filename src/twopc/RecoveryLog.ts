import { appendFileSync, existsSync, readFileSync } from 'node:fs';

export type RecoveryStatus = 'PREPARED' | 'COMMIT_DECISION' | 'COMMITTED' | 'ABORTED';

type RecoveryRecord = {
  transactionId: string;
  status: RecoveryStatus;
  participants: string[];
  timestamp: string;
};

export class RecoveryLog {
  constructor(private readonly filePath = process.env.RECOVERY_LOG_FILE ?? '2pc-recovery.log') { }

  append(transactionId: string, status: RecoveryStatus, participants: string[]): void {
    const record: RecoveryRecord = {
      transactionId,
      status,
      participants,
      timestamp: new Date().toISOString(),
    };
    appendFileSync(this.filePath, `${JSON.stringify(record)}\n`, 'utf8');
  }

  unfinished(): RecoveryRecord[] {
    if (!existsSync(this.filePath)) return [];
    const latest = new Map<string, RecoveryRecord>();
    for (const line of readFileSync(this.filePath, 'utf8').split('\n').filter(Boolean)) {
      const record = JSON.parse(line) as RecoveryRecord;
      latest.set(record.transactionId, record);
    }
    return [...latest.values()].filter(({ status }) => status === 'PREPARED' || status === 'COMMIT_DECISION');
  }
}
