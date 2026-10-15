import type postgres from "postgres";

export type PostgresMigration = {
	readonly version: number;
	readonly statements: readonly string[];
};

const INITIAL_SCHEMA: readonly string[] = [
	`CREATE TABLE durable_metadata (
		store_id TEXT COLLATE "C" PRIMARY KEY,
		next_id BIGINT NOT NULL,
		next_seq BIGINT NOT NULL
	)`,
	`INSERT INTO durable_metadata (store_id, next_id, next_seq) VALUES ('default', '2', 1)`,
	`CREATE TABLE record_ids (
		store_id TEXT COLLATE "C" NOT NULL,
		id BIGINT NOT NULL,
		record_type TEXT NOT NULL CHECK (record_type IN ('conversation', 'entry', 'task', 'submission', 'document')),
		PRIMARY KEY (store_id, id)
	)`,
	`CREATE TABLE conversations (
		store_id TEXT COLLATE "C" NOT NULL,
		id BIGINT NOT NULL,
		owner_conversation_id BIGINT,
		owner_task_id BIGINT,
		record JSON NOT NULL,
		PRIMARY KEY (store_id, id)
	)`,
	"CREATE INDEX conversations_by_owner_conversation ON conversations (store_id, owner_conversation_id, id)",
	"CREATE INDEX conversations_by_owner_task ON conversations (store_id, owner_task_id, id)",
	`CREATE TABLE entries (
		store_id TEXT COLLATE "C" NOT NULL,
		id BIGINT NOT NULL,
		conversation_id BIGINT NOT NULL,
		head BIGINT,
		commit_seq BIGINT NOT NULL,
		record JSON NOT NULL,
		PRIMARY KEY (store_id, id)
	)`,
	"CREATE INDEX entries_by_conversation ON entries (store_id, conversation_id, id DESC)",
	"CREATE INDEX entry_heads_by_conversation ON entries (store_id, conversation_id, id DESC) WHERE head IS NOT NULL",
	`CREATE TABLE tasks (
		store_id TEXT COLLATE "C" NOT NULL,
		id BIGINT NOT NULL,
		conversation_id BIGINT NOT NULL,
		kind TEXT COLLATE "C" NOT NULL,
		status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'completing', 'terminal')),
		abort_requested BOOLEAN NOT NULL,
		background BOOLEAN NOT NULL,
		record JSON NOT NULL,
		PRIMARY KEY (store_id, id)
	)`,
	"CREATE INDEX tasks_by_status ON tasks (store_id, status, id)",
	"CREATE INDEX tasks_by_conversation ON tasks (store_id, conversation_id, id)",
	"CREATE INDEX tasks_by_kind ON tasks (store_id, md5(kind), id)",
	"CREATE INDEX tasks_by_abort_requested ON tasks (store_id, abort_requested, id)",
	"CREATE INDEX tasks_by_background ON tasks (store_id, background, id)",
	`CREATE TABLE submissions (
		store_id TEXT COLLATE "C" NOT NULL,
		id BIGINT NOT NULL,
		conversation_id BIGINT NOT NULL,
		request_id TEXT COLLATE "C",
		status TEXT NOT NULL CHECK (status IN ('queued', 'placed', 'done', 'unanswered')),
		record JSON NOT NULL,
		PRIMARY KEY (store_id, id)
	)`,
	"CREATE INDEX submissions_by_request ON submissions (store_id, conversation_id, md5(request_id))",
	"CREATE INDEX submissions_by_conversation ON submissions (store_id, conversation_id, id)",
	"CREATE INDEX submissions_by_status ON submissions (store_id, status, id)",
	`CREATE TABLE documents (
		store_id TEXT COLLATE "C" NOT NULL,
		id BIGINT NOT NULL,
		kind TEXT COLLATE "C" NOT NULL,
		family BIGINT NOT NULL CHECK (family IN (0, 1)),
		key_value TEXT COLLATE "C" NOT NULL,
		scope_kind TEXT NOT NULL CHECK (scope_kind IN ('session', 'conversation', 'task')),
		owner_id BIGINT NOT NULL,
		created_at BIGINT NOT NULL,
		retired_at BIGINT,
		record JSON NOT NULL,
		PRIMARY KEY (store_id, id)
	)`,
	`CREATE INDEX documents_by_address
		ON documents (store_id, md5(kind), scope_kind, owner_id, family, md5(key_value), created_at DESC, retired_at)`,
	"CREATE INDEX documents_by_scope ON documents (store_id, scope_kind, owner_id, id)",
	"CREATE INDEX documents_by_scope_kind ON documents (store_id, scope_kind, owner_id, md5(kind), id)",
	`CREATE TABLE document_revisions (
		store_id TEXT COLLATE "C" NOT NULL,
		document_id BIGINT NOT NULL,
		seq BIGINT NOT NULL,
		kind TEXT NOT NULL CHECK (kind IN ('base', 'delta')),
		version BIGINT NOT NULL,
		content JSON NOT NULL,
		PRIMARY KEY (store_id, document_id, seq)
	)`,
	"CREATE INDEX document_revisions_by_kind ON document_revisions (store_id, document_id, kind, seq DESC)",
];

/** Immutable, ordered schema history. Append new migrations after the initial schema ships. */
export const POSTGRES_MIGRATIONS: readonly PostgresMigration[] = [{ version: 1, statements: INITIAL_SCHEMA }];

export const CURRENT_POSTGRES_SCHEMA_VERSION = POSTGRES_MIGRATIONS.at(-1)?.version ?? 0;

type SchemaRow = { readonly version: string };

/** Apply all pending schema migrations atomically. */
export async function applyPostgresMigrations(
	database: postgres.Sql,
	migrations: readonly PostgresMigration[] = POSTGRES_MIGRATIONS,
): Promise<void> {
	for (let index = 0; index < migrations.length; index++) {
		if (migrations[index]?.version !== index + 1) {
			throw new Error("Durable PostgreSQL migrations must have contiguous versions starting at 1");
		}
	}

	await database.begin(async (transaction) => {
		await transaction.unsafe(`CREATE TABLE IF NOT EXISTS durable_schema (
			singleton BIGINT PRIMARY KEY CHECK (singleton = 1),
			version BIGINT NOT NULL CHECK (version >= 0)
		)`);
		await transaction`INSERT INTO durable_schema (singleton, version) VALUES (1, 0) ON CONFLICT DO NOTHING`;
		const [row] = await transaction<SchemaRow[]>`SELECT version FROM durable_schema WHERE singleton = 1`;
		if (row === undefined) throw new Error("Durable PostgreSQL schema metadata is missing");
		const currentVersion = migrations.at(-1)?.version ?? 0;
		if (Number(row.version) > currentVersion) {
			throw new Error(
				`Durable PostgreSQL schema version ${row.version} is newer than supported version ${currentVersion}`,
			);
		}
		for (const migration of migrations) {
			if (migration.version <= Number(row.version)) continue;
			for (const statement of migration.statements) await transaction.unsafe(statement);
			await transaction`UPDATE durable_schema SET version = ${migration.version} WHERE singleton = 1`;
		}
	});
}
