import 'dotenv/config';
import { MongoClient } from 'mongodb';
import { Pool } from 'pg';

const postgresUrl = process.env.POSTGRES_URL;
const mongoUrl = process.env.MONGO_URL ?? 'mongodb://localhost:27017';
const mongoDatabase = process.env.MONGO_DATABASE ?? 'dac';

if (!postgresUrl) throw new Error('POSTGRES_URL is required');

const postgres = new Pool({ connectionString: postgresUrl });
const mongo = new MongoClient(mongoUrl);

async function clean(): Promise<void> {
  console.log('Limpando PostgreSQL...');
  await postgres.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public;');
  console.log('PostgreSQL limpo.');

  console.log(`Limpando MongoDB (${mongoDatabase})...`);
  await mongo.connect();
  await mongo.db(mongoDatabase).dropDatabase();
  console.log('MongoDB limpo.');
}

clean()
  .then(() => console.log('Limpeza concluida.'))
  .catch((error) => {
    console.error('Falha ao limpar os bancos:', error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await Promise.allSettled([postgres.end(), mongo.close()]);
  });
