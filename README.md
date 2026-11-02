# Persistência distribuída com 2PC

Exemplo em TypeScript/Node.js de coordenação de operações entre PostgreSQL e MongoDB. O projeto usa staging (shadow data) e locks explícitos, uma estratégia que mantém o protocolo simétrico entre os dois bancos.

## Arquitetura

- `src/main.ts`: unidade principal; define a tabela/coleção customizada, chama inserções/updates/deletes e executa o observador a cada 10 ms.
- `src/twopc/TwoPhaseCommit.ts`: unidade de abstração; gera o `transactionId`, executa `prepare`, depois `commit`, e compensa os participantes preparados com `rollback` quando a preparação falha.
- `src/orm/TwoPhaseRepository.ts`: camada ORM fina; uma subclasse recebe a definição da tabela no construtor e expõe `insert`, `update`, `read` e `delete` sobre o 2PC.
- `src/connection/PostgresConnection.ts` e `src/connection/MongoConnection.ts`: unidade de conexão; encapsula SQL/driver Mongo, cria estruturas de dados dinâmicas e adiciona os campos de lock.

Cada participante mantém uma tabela/coleção shadow com nome `twopc_shadow_<hash da transação>`. No `prepare`, a operação é validada e escrita nessa estrutura; updates e deletes também marcam a linha/documento original com `__twopc_lock_tx_id`. No `commit`, a shadow é publicada no dado principal e removida. No `rollback`, a shadow é removida e o lock é liberado. No startup, todas as shadows com esse prefixo são tratadas como órfãs, removidas e seus locks são limpos.

O relatório também registra o ciclo detalhado de cada transação (`PREPARE`, ACKs, `COMMIT`, `ROLLBACK`) e quando o polling fica bloqueado aguardando a conclusão dos participantes.

O coordenador mantém um recovery log em `RECOVERY_LOG_FILE`. Se o processo cair depois de registrar `COMMIT_DECISION`, o startup repete os commits de forma idempotente; se cair apenas depois de `PREPARED`, o startup aborta e limpa os participantes preparados. A transação ORM também oferece read-your-writes: leituras de uma chave alterada na própria transação retornam a última versão pendente antes do commit.

A definição da tabela fica na classe concreta. O construtor do repositório recebe as conexões abstratas, cria a tabela/coleção nas duas bases e aguarda essa inicialização antes das operações:

```typescript
class Inventory extends TwoPhaseRepository {
	private static instance: Inventory | null = null;

	static getInstance(connections: TwoPhaseParticipant[]): Inventory {
		Inventory.instance ??= new Inventory(connections);
		return Inventory.instance;
	}

	private constructor(connections: TwoPhaseParticipant[]) {
		super(connections, { name: 'inventory', keyColumn: 'sku', columns: { sku: 'text', quantity: 'integer' } });
	}
}

const inventory = Inventory.getInstance([postgres, mongo]);
await inventory.ready();
const result = await inventory.read('keyboard-01', ['sku', 'quantity']);
```

Os snapshots completos continuam sendo usados somente pelo relatório e pelo polling configurado para comparar o estado inteiro dos dois bancos.

Para agrupar várias operações em uma única transação 2PC, use a fábrica do repositório. As operações são preparadas nos dois bancos e `commit()` só retorna depois dos dois participantes confirmarem:

```typescript
const transaction = inventory.createTransaction();
transaction.insert({ sku: 'keyboard-01', description: 'Keyboard', quantity: 10, price: 249.9, active: true });
await transaction.update('mouse-01', { quantity: 30 });
await transaction.read('monitor-01', ['sku', 'quantity']);
transaction.delete('obsolete-01');
await transaction.commit();
```

## Executar

Pré-requisitos: Node.js 20+, npm e Docker.

```bash
cp .env.example .env
npm install
docker compose up -d
npm run build
npm start
```

O modo de leitura pode ser alternado no `.env`:

```env
READ_MODE=blocking
```

- `blocking`: leituras aguardam o commit ou rollback completo dos dois participantes.
- `mvcc`: leituras retornam o snapshot capturado antes do `prepare` enquanto a operação está em andamento; o novo estado só aparece depois que todos os commits terminam.

O prepare pode ser executado concorrentemente com:

```env
PARALLEL_PREPARE=true
```

Com `false`, PostgreSQL e MongoDB são preparados na ordem configurada. Com `true`, as chamadas de I/O são iniciadas juntas e o coordenador aguarda ambas. Em Node.js isso é concorrência assíncrona do event loop, não criação de threads JavaScript dedicadas.

Para apagar todos os dados dos dois bancos:

```bash
npm run clean
```

Esse comando recria o schema `public` do PostgreSQL e executa `dropDatabase()` no banco MongoDB configurado. A operação é destrutiva.

O `main` demonstra e registra no console e em `2pc-report.txt`:

1. insert PostgreSQL -> MongoDB;
2. falha no MongoDB e rollback do staging PostgreSQL;
3. insert MongoDB -> PostgreSQL;
4. falha no PostgreSQL e rollback do staging MongoDB;
5. falhas de update em produtos reais, com rollback nos dois sentidos;
6. três updates nos dois bancos enquanto leituras são feitas a cada 10 ms; o log mostra apenas o total de leituras e divergências para evitar ruído;
7. delete coordenado nos dois bancos.

Para desenvolvimento, `npm run dev` executa diretamente o TypeScript com `tsx`.

Os testes podem ser executados com:

```bash
npm test
```

Eles cobrem blocking, MVCC, prepare paralelo, read-your-writes, commit agrupado e rollback de prepare parcial.
