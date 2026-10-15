import { randomUUID } from "node:crypto";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import postgres from "postgres";
import { afterEach } from "vitest";
import { PostgresStorage } from "../src/storage/postgres/index.ts";

const cleanups: Array<() => Promise<void>> = [];

afterEach(async () => {
	try {
		for (const cleanup of cleanups) await cleanup();
	} finally {
		cleanups.length = 0;
	}
});

/** Each test gets an isolated store; the configured database must be a disposable test database. */
export async function createPostgresDatabase() {
	const url = process.env.PI_DURABLE_POSTGRES_URL;
	if (!url) throw new Error("Set PI_DURABLE_POSTGRES_URL to a local PostgreSQL test database");
	const schema = `pi_storage_${randomUUID().replaceAll("-", "")}`;
	const admin = postgres(url, { max: 1, onnotice: () => {} });
	const storages = new Set<PostgresStorage>();
	const clients = new Set<postgres.Sql>();
	const connect = (applicationName = schema) => {
		const client = postgres(url, {
			connection: { search_path: schema, application_name: applicationName },
			onnotice: () => {},
		});
		clients.add(client);
		return client;
	};
	cleanups.push(async () => {
		try {
			for (const storage of storages) await storage.close(BACKGROUND_CONTEXT);
			for (const client of clients) await client.end();
			await admin`DROP SCHEMA IF EXISTS ${admin(schema)} CASCADE`;
		} finally {
			await admin.end();
		}
	});
	await admin`CREATE SCHEMA ${admin(schema)}`;
	const sql = connect(`${schema}_inspect`);
	return {
		sql,
		schema,
		open: async () => {
			const storage = await PostgresStorage.open(connect());
			storages.add(storage);
			return storage;
		},
	};
}
