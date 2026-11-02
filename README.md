# Persistência distribuída com 2PC

Exemplo em TypeScript/Node.js de coordenação de operações entre PostgreSQL e MongoDB sem usar as instruções de transação nativas do PostgreSQL. O projeto usa staging (shadow data) e locks explícitos, uma estratégia que mantém o protocolo simétrico entre os dois bancos.

## Arquitetura

- `src/main.ts`: unidade principal; define a tabela/coleção customizada, chama inserções/updates/deletes e executa o observador a cada 10 ms.
- `src/twopc/TwoPhaseCommit.ts`: unidade de abstração; gera o `transactionId`, executa `prepare`, depois `commit`, e compensa os participantes preparados com `rollback` quando a preparação falha.
- `src/connection/PostgresConnection.ts` e `src/connection/MongoConnection.ts`: unidade de conexão; encapsula SQL/driver Mongo, cria estruturas de dados dinâmicas e adiciona os campos de lock.

Cada participante mantém uma tabela/coleção shadow com nome `twopc_shadow_<hash da transação>`. No `prepare`, a operação é validada e escrita nessa estrutura; updates e deletes também marcam a linha/documento original com `__twopc_lock_tx_id`. No `commit`, a shadow é publicada no dado principal e removida. No `rollback`, a shadow é removida e o lock é liberado. No startup, todas as shadows com esse prefixo são tratadas como órfãs, removidas e seus locks são limpos.

## Executar

Pré-requisitos: Node.js 20+, npm e Docker.

```bash
cp .env.example .env
npm install
docker compose up -d
npm run build
npm start
```

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

## Escopo da garantia

Este é um coordenador 2PC didático e não usa transações locais. A fase `prepare` é compensável e não publica dados principais. Para tolerar a queda do processo entre commits, uma implementação de produção deve persistir uma decisão `PREPARED`/`COMMITTED` em um recovery log durável e adicionar uma rotina de recuperação idempotente. Sem esse log, nenhuma implementação baseada apenas em chamadas ao driver consegue garantir atomicidade após a queda do coordenador no meio da fase de commit.