import { TwoPhaseRepository } from './TwoPhaseRepository.js';
import type { ReadMode, TwoPhaseParticipant } from '../types.js';

export class Inventory extends TwoPhaseRepository {
  private static instance: Inventory | null = null;

  static getInstance(connections: readonly TwoPhaseParticipant[], onEvent: (message: string) => void = () => undefined, readMode: ReadMode = 'blocking', parallelPrepare = false): Inventory {
    Inventory.instance ??= new Inventory(connections, onEvent, readMode, parallelPrepare);
    return Inventory.instance;
  }

  private constructor(connections: readonly TwoPhaseParticipant[], onEvent: (message: string) => void, readMode: ReadMode, parallelPrepare: boolean) {
    super(connections, {
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
    }, onEvent, readMode, undefined, parallelPrepare);
  }
}