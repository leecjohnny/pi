import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { describe, expect, it } from "vitest";
import { idFromNumber } from "../src/ids.ts";
import {
	applyPostgresMigrations,
	CURRENT_POSTGRES_SCHEMA_VERSION,
	POSTGRES_MIGRATIONS,
	type PostgresMigration,
} from "../src/storage/postgres/index.ts";
import { type EntryId, ROOT_CONVERSATION_ID } from "../src/types.ts";
import { createPostgresDatabase } from "./postgres-fixture.ts";

describe("durable PostgreSQL migrations", () => {
	it("creates the current schema and can be applied repeatedly", async () => {
		const { sql: database } = await createPostgresDatabase();
		await applyPostgresMigrations(database);
		await applyPostgresMigrations(database);
		expect((await database`SELECT version FROM durable_schema WHERE singleton = 1`)[0]).toEqual({
			version: String(CURRENT_POSTGRES_SCHEMA_VERSION),
		});
		expect((await database`SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1`)[0]).toEqual({
			next_id: "2",
			next_seq: "1",
		});
	});

	it("rejects a database newer than the portable core", async () => {
		const { sql: database, open } = await createPostgresDatabase();
		await applyPostgresMigrations(database);
		await database`UPDATE durable_schema SET version = ${CURRENT_POSTGRES_SCHEMA_VERSION + 1} WHERE singleton = 1`;

		await expect(open()).rejects.toThrow("is newer than supported version");
	});

	it("rolls initial bootstrap and every pending migration back together", async () => {
		const { sql: database } = await createPostgresDatabase();
		const failed: readonly PostgresMigration[] = [
			{
				version: 1,
				statements: [
					"CREATE TABLE migration_first (value TEXT)",
					"INSERT INTO migration_first (value) VALUES ('retained')",
				],
			},
			{
				version: 2,
				statements: ["CREATE TABLE migration_second (value TEXT)", "THIS IS NOT SQL"],
			},
		];
		await expect(applyPostgresMigrations(database, failed)).rejects.toThrow();
		expect(
			(
				await database`SELECT count(*)::integer AS count FROM information_schema.tables WHERE table_schema = current_schema() AND table_name IN ('durable_schema', 'migration_first', 'migration_second')`
			)[0],
		).toEqual({ count: 0 });

		await applyPostgresMigrations(database, [
			failed[0],
			{ version: 2, statements: ["CREATE TABLE migration_second (value TEXT)"] },
		]);
		expect((await database`SELECT version FROM durable_schema WHERE singleton = 1`)[0]).toEqual({
			version: "2",
		});
		expect((await database`SELECT value FROM migration_first`)[0]).toEqual({
			value: "retained",
		});
	});

	it("rolls a failed migration back and preserves stored data for a successful retry", async () => {
		const { sql: database, open } = await createPostgresDatabase();
		const storage = await open();
		await storage.commit(
			[
				{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } },
				{
					type: "entry",
					value: {
						id: idFromNumber<EntryId>(2),
						conversationId: ROOT_CONVERSATION_ID,
						kind: "retained",
						data: { retained: true },
					},
				},
			],
			BACKGROUND_CONTEXT,
		);
		await storage.close(BACKGROUND_CONTEXT);

		const nextVersion = CURRENT_POSTGRES_SCHEMA_VERSION + 1;
		const failedMigrations: readonly PostgresMigration[] = [
			...POSTGRES_MIGRATIONS,
			{
				version: nextVersion,
				statements: ["CREATE TABLE migration_probe (value TEXT)", "THIS IS NOT SQL"],
			},
		];
		await expect(applyPostgresMigrations(database, failedMigrations)).rejects.toThrow();
		expect((await database`SELECT version FROM durable_schema WHERE singleton = 1`)[0]).toEqual({
			version: String(CURRENT_POSTGRES_SCHEMA_VERSION),
		});
		expect(
			(
				await database`SELECT count(*)::integer AS count FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'migration_probe'`
			)[0],
		).toEqual({ count: 0 });

		const successfulMigrations: readonly PostgresMigration[] = [
			...POSTGRES_MIGRATIONS,
			{ version: nextVersion, statements: ["CREATE TABLE migration_probe (value TEXT)"] },
		];
		await applyPostgresMigrations(database, successfulMigrations);
		expect((await database`SELECT version FROM durable_schema WHERE singleton = 1`)[0]).toEqual({
			version: String(nextVersion),
		});
		expect((await database`SELECT record, commit_seq FROM entries WHERE id = 2`)[0]).toEqual({
			record: {
				id: 2,
				conversationId: ROOT_CONVERSATION_ID,
				kind: "retained",
				data: { retained: true },
			},
			commit_seq: "1",
		});
		expect((await database`SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1`)[0]).toEqual({
			next_id: "3",
			next_seq: "2",
		});
	});
});
