import type { Context, JsonValue } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { apply, type Op } from "@earendil-works/chord/delta";
import type postgres from "postgres";
import { StorageRequestError } from "../../errors.ts";
import { idFromNumber, seqFromNumber } from "../../ids.ts";
import type {
	ConversationId,
	ConversationQuery,
	ConversationRecord,
	Cursor,
	DocumentAddress,
	DocumentContent,
	DocumentCreate,
	DocumentId,
	DocumentPoint,
	DocumentQuery,
	DocumentRecord,
	EntryId,
	EntryQuery,
	EntryRecord,
	Id,
	JsonObject,
	Page,
	ScanOrder,
	Seq,
	Storage,
	StorageWrite,
	StoredDocument,
	SubmissionId,
	SubmissionQuery,
	SubmissionRecord,
	TaskId,
	TaskQuery,
	TaskRecord,
} from "../../types.ts";
import { nextCursor, scanStart } from "../scan.ts";
import { applyPostgresMigrations } from "./migrations.ts";

type StoredTask = TaskRecord<JsonValue, JsonValue, JsonValue>;
type TableName = "conversation" | "entry" | "task" | "submission" | "document";
type RevisionRow = {
	readonly seq: string;
	readonly kind: DocumentContent["kind"];
	readonly version: string;
	readonly content: JsonObject | readonly Op[];
};
type DocumentRow = { readonly record: DocumentRecord } & (
	| RevisionRow
	| { readonly seq: null; readonly kind: null; readonly version: null; readonly content: null }
);
type DocumentMetadata = { readonly record: DocumentRecord; readonly version: string | null };

type DocumentAction = {
	create?: DocumentCreate;
	copy?: Extract<StorageWrite, { readonly type: "document.copy" }>["source"];
	content?: DocumentContent;
	retire: boolean;
};
type ScopeColumns = {
	readonly scopeKind: DocumentRecord["scope"]["kind"];
	readonly ownerId: number;
};

// JSON encoding preserves NUL and lone UTF-16 surrogates in indexed text identities.
const encodeIndexedString = (value: string): string => JSON.stringify(value);

const cursorId = <I extends Id<string>>(cursor: Cursor | undefined): I | undefined => {
	const after = cursor?.after;
	if (after === undefined) return undefined;
	if (typeof after !== "number" || !Number.isSafeInteger(after))
		throw new StorageRequestError("Invalid storage cursor");
	return idFromNumber<I>(after);
};

const page = <T extends { readonly id: Id<string> }>(
	values: readonly T[],
	limit: number,
	order: ScanOrder = "ascending",
): Page<T, Cursor> => {
	const items = values.slice(0, limit);
	if (values.length <= limit) return { items };
	return { items, next: nextCursor(items.at(-1)!.id, order) };
};

const scopeColumns = (scope: DocumentRecord["scope"]): ScopeColumns => {
	switch (scope.kind) {
		case "session":
			return { scopeKind: "session", ownerId: 0 };
		case "conversation":
			return { scopeKind: "conversation", ownerId: scope.conversationId };
		case "task":
			return { scopeKind: "task", ownerId: scope.taskId };
	}
};

const addressParts = (address: DocumentAddress | DocumentCreate | DocumentRecord) => {
	const scope = scopeColumns(address.scope);
	return {
		kind: encodeIndexedString(address.kind),
		...scope,
		family: address.key === undefined ? 0 : 1,
		keyValue: encodeIndexedString(address.key ?? ""),
	};
};

const addressKey = (address: DocumentAddress | DocumentCreate | DocumentRecord): string => {
	const parts = addressParts(address);
	return JSON.stringify([parts.kind, parts.scopeKind, parts.ownerId, parts.family, parts.keyValue]);
};

const isAliveAt = (record: DocumentRecord, at: DocumentPoint): boolean => {
	if (at === "current") return record.retiredAt === undefined;
	return record.createdAt <= at && (record.retiredAt === undefined || at < record.retiredAt);
};

const isCurrentOnly = (record: DocumentRecord): boolean =>
	record.scope.kind !== "conversation" || record.history === "latest";

/** One migrated schema and pool, shared by independent native stores. */
export interface PostgresBackend {
	openStore(storeId: string): Promise<PostgresStorage>;
	close(): Promise<void>;
}

/** PostgreSQL implementation of native Storage, bound to one store. */
export class PostgresStorage implements Storage {
	private readonly sql: postgres.Sql;
	private readonly storeId: string;
	private readonly release: () => void;
	private ownsPool = false;
	private closed = false;
	private closing: Promise<void> | undefined;
	private active = 0;
	private drained: (() => void) | undefined;

	private constructor(sql: postgres.Sql, storeId: string, release: () => void) {
		this.sql = sql;
		this.storeId = storeId;
		this.release = release;
	}

	/** Open the default store, taking ownership of its dedicated pool. */
	static async open(sql: postgres.Sql): Promise<PostgresStorage> {
		const backend = await PostgresStorage.openBackend(sql);
		try {
			const storage = await backend.openStore("default");
			storage.ownsPool = true;
			return storage;
		} catch (error) {
			await backend.close();
			throw error;
		}
	}

	/** Migrate an inactive backend. The host ensures one owning Session per store ID. */
	static async openBackend(sql: postgres.Sql): Promise<PostgresBackend> {
		try {
			await applyPostgresMigrations(sql);
		} catch (error) {
			try {
				await sql.end();
			} catch {
				// Preserve the initialization failure.
			}
			throw error;
		}
		const stores = new Set<PostgresStorage>();
		let closed = false;
		let closing: Promise<void> | undefined;
		return {
			async openStore(storeId) {
				if (closed) throw new Error("PostgresBackend is closed");
				const storage = new PostgresStorage(sql, storeId, () => stores.delete(storage));
				stores.add(storage);
				try {
					await storage.admit(async () => {
						const [metadata] = await sql`WITH initialized AS (
							INSERT INTO durable_metadata (store_id, next_id, next_seq)
							SELECT ${storeId}, 2, 1 WHERE NOT EXISTS (SELECT 1 FROM record_ids WHERE store_id = ${storeId})
							ON CONFLICT (store_id) DO NOTHING RETURNING next_id, next_seq
						) SELECT next_id, next_seq FROM initialized
						UNION ALL SELECT next_id, next_seq FROM durable_metadata WHERE store_id = ${storeId}`;
						if (metadata === undefined) throw new Error("Durable PostgreSQL metadata is missing");
					});
					return storage;
				} catch (error) {
					await storage.close(BACKGROUND_CONTEXT);
					throw error;
				}
			},
			close() {
				if (closing === undefined) {
					closed = true;
					closing = (async () => {
						await Promise.all([...stores].map((store) => store.close(BACKGROUND_CONTEXT)));
						await sql.end();
					})();
				}
				return closing;
			},
		};
	}

	commit(writes: readonly StorageWrite[], _context: Context): Promise<Seq> {
		return this.admit(async () => {
			const actions = this.prepareDocumentActions(writes);
			return this.sql.begin(async (sql) => {
				const claimed = await this.claimGlobalIds(sql, writes);
				const documents = await this.checkDocumentActions(sql, actions);
				let nextId = 2;
				for (const id of claimed.keys()) nextId = Math.max(nextId, id + 1);
				const [metadata] = await sql<{ seq: string }[]>`UPDATE durable_metadata
					SET next_seq = next_seq + 1, next_id = GREATEST(next_id, ${nextId}::bigint)
					WHERE store_id = ${this.storeId} RETURNING next_seq - 1 AS seq`;
				if (metadata === undefined) throw new Error("Durable PostgreSQL metadata is missing");
				const seq = seqFromNumber(Number(metadata.seq));
				await this.applyTableWrites(sql, writes, seq);
				await this.applyDocumentActions(sql, actions, documents, seq);
				return seq;
			});
		});
	}

	mintId<I extends Id<string>>(): Promise<I> {
		return this.admit(async () => {
			const [row] = await this.sql<{ id: string }[]>`UPDATE durable_metadata SET next_id = next_id + 1
				WHERE store_id = ${this.storeId} AND next_id <= ${Number.MAX_SAFE_INTEGER}::bigint RETURNING next_id - 1 AS id`;
			if (row === undefined) throw new Error("ID space is exhausted");
			return idFromNumber<I>(Number(row.id));
		});
	}

	conversation(id: ConversationId, _context: Context): Promise<ConversationRecord | undefined> {
		return this.admit(async () => {
			const [row] = await this.sql<
				{ record: ConversationRecord }[]
			>`SELECT record FROM conversations WHERE store_id = ${this.storeId} AND id = ${id}`;
			return row?.record;
		});
	}

	scanConversations(
		query: ConversationQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<ConversationRecord, Cursor>> {
		return this.admit(async () => {
			const sql = this.sql;
			const start = scanStart(query.order, cursor, "ascending");
			const rows = await sql<{ record: ConversationRecord }[]>`SELECT record FROM conversations
				WHERE store_id = ${this.storeId} AND ${scanCondition(sql, start.after, start.order)}
				${query.ownerConversationId === undefined ? sql`` : sql`AND owner_conversation_id = ${query.ownerConversationId}`}
				${query.ownerTaskId === undefined ? sql`` : sql`AND owner_task_id = ${query.ownerTaskId}`}
				ORDER BY id ${direction(sql, start.order)} LIMIT ${limit + 1}`;
			return page(
				rows.map((row) => row.record),
				limit,
				start.order,
			);
		});
	}

	entry(id: EntryId, context: Context): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined>;
	entry(
		conversationId: ConversationId,
		id: EntryId,
		context: Context,
	): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined>;
	entry(
		idOrConversationId: EntryId | ConversationId,
		idOrContext: EntryId | Context,
		context?: Context,
	): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined> {
		return this.admit(async () => {
			const id =
				context === undefined
					? idFromNumber<EntryId>(idOrConversationId)
					: typeof idOrContext === "number"
						? idFromNumber<EntryId>(idOrContext)
						: undefined;
			if (id === undefined) throw new StorageRequestError("Storage.entry() requires an entry ID");
			const sql = this.sql;
			const conversationId = context === undefined ? undefined : idFromNumber<ConversationId>(idOrConversationId);
			const [row] =
				conversationId === undefined
					? await sql<{ record: EntryRecord | null; commit_seq: string }[]>`
					SELECT record, commit_seq FROM entries WHERE store_id = ${this.storeId} AND id = ${id}`
					: await sql<{ record: EntryRecord | null; commit_seq: string }[]>`${this.lineage(conversationId)}
					SELECT entry.record, entry.commit_seq FROM entries entry JOIN lineage
					ON entry.conversation_id = lineage.id AND entry.id <= lineage.upper_id
					WHERE entry.store_id = ${this.storeId} AND entry.id = ${id}
					UNION ALL SELECT NULL::json, NULL::bigint WHERE NOT EXISTS (SELECT 1 FROM lineage)`;
			if (row?.record === null) throw new StorageRequestError(`Unknown conversation: ${conversationId}`);
			return row === undefined ? undefined : { entry: row.record, commitSeq: seqFromNumber(Number(row.commit_seq)) };
		});
	}

	findLatestHeadMarker(
		conversationId: ConversationId,
		atOrBeforeEntryId: EntryId | undefined,
		_context: Context,
	): Promise<(EntryRecord & { readonly head: EntryId }) | undefined> {
		return this.admit(async () => {
			const sql = this.sql;
			const [row] = await sql<
				{ record: (EntryRecord & { readonly head: EntryId }) | null }[]
			>`${this.lineage(conversationId)}
				SELECT record FROM (
					SELECT heads.record, lineage.depth, heads.id FROM lineage CROSS JOIN LATERAL (
						SELECT record, id FROM entries WHERE store_id = ${this.storeId} AND conversation_id = lineage.id
						AND head IS NOT NULL AND id <= LEAST(lineage.upper_id, ${atOrBeforeEntryId ?? Number.MAX_SAFE_INTEGER}::bigint)
						ORDER BY id DESC LIMIT 1
					) heads
					UNION ALL SELECT NULL::json, 0, NULL::bigint WHERE NOT EXISTS (SELECT 1 FROM lineage)
				) visible ORDER BY depth, id DESC LIMIT 1`;
			if (row?.record === null) throw new StorageRequestError(`Unknown conversation: ${conversationId}`);
			return row?.record;
		});
	}

	scanEntries(
		query: EntryQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<EntryRecord, Cursor>> {
		return this.admit(async () => {
			const start = scanStart(query.order, cursor, "descending");
			const sql = this.sql;
			const rows = await sql<{ record: EntryRecord | null }[]>`${this.lineage(query.conversationId)}
				SELECT record FROM (
					SELECT entries.record, lineage.depth, entries.id FROM lineage CROSS JOIN LATERAL (
						SELECT record, id FROM entries WHERE store_id = ${this.storeId} AND conversation_id = lineage.id
						AND id <= LEAST(lineage.upper_id, ${query.maxEntryId ?? Number.MAX_SAFE_INTEGER}::bigint)
						${query.minEntryId === undefined ? sql`` : sql`AND id >= ${query.minEntryId}`}
						AND ${scanCondition(sql, start.after, start.order)}
						ORDER BY id ${direction(sql, start.order)} LIMIT ${limit + 1}
					) entries
					UNION ALL SELECT NULL::json, 0, NULL::bigint WHERE NOT EXISTS (SELECT 1 FROM lineage)
				) visible ORDER BY depth ${direction(sql, start.order === "ascending" ? "descending" : "ascending")},
					id ${direction(sql, start.order)} LIMIT ${limit + 1}`;
			if (rows[0]?.record === null) throw new StorageRequestError(`Unknown conversation: ${query.conversationId}`);
			return page(
				rows.map((row) => row.record!),
				limit,
				start.order,
			);
		});
	}

	private lineage(id: ConversationId) {
		const sql = this.sql;
		return sql`WITH RECURSIVE lineage AS (
			SELECT id, record, 0 AS depth, ${Number.MAX_SAFE_INTEGER}::bigint AS upper_id
			FROM conversations WHERE store_id = ${this.storeId} AND id = ${id}
			UNION ALL
			SELECT parent.id, parent.record, child.depth + 1,
				LEAST(child.upper_id, (child.record->'parent'->>'at')::bigint)
			FROM lineage child JOIN conversations parent ON parent.store_id = ${this.storeId} AND parent.id = (child.record->'parent'->>'conversationId')::bigint
		)`;
	}

	task(id: TaskId, _context: Context): Promise<StoredTask | undefined> {
		return this.admit(async () => {
			const [row] = await this.sql<
				{ record: StoredTask }[]
			>`SELECT record FROM tasks WHERE store_id = ${this.storeId} AND id = ${id}`;
			return row?.record;
		});
	}

	scanTasks(
		query: TaskQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<StoredTask, Cursor>> {
		return this.admit(async () => {
			const sql = this.sql;
			const start = scanStart(query.order, cursor, "ascending");
			const kind = query.kind === undefined ? undefined : encodeIndexedString(query.kind);
			const rows = await sql<{ record: StoredTask }[]>`SELECT record FROM tasks
				WHERE store_id = ${this.storeId} AND ${scanCondition(sql, start.after, start.order)}
				${query.conversationId === undefined ? sql`` : sql`AND conversation_id = ${query.conversationId}`}
				${kind === undefined ? sql`` : sql`AND md5(kind) = md5(${kind}) AND kind = ${kind}`}
				${query.status === undefined ? sql`` : sql`AND status = ${query.status}`}
				${query.abortRequested === undefined ? sql`` : sql`AND abort_requested = ${query.abortRequested}`}
				${query.background === undefined ? sql`` : sql`AND background = ${query.background}`}
				ORDER BY id ${direction(sql, start.order)} LIMIT ${limit + 1}`;
			return page(
				rows.map((row) => row.record),
				limit,
				start.order,
			);
		});
	}

	submission(id: SubmissionId, _context: Context): Promise<SubmissionRecord | undefined> {
		return this.admit(async () => {
			const [row] = await this.sql<
				{ record: SubmissionRecord }[]
			>`SELECT record FROM submissions WHERE store_id = ${this.storeId} AND id = ${id}`;
			return row?.record;
		});
	}

	scanSubmissions(
		query: SubmissionQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<SubmissionRecord, Cursor>> {
		return this.admit(async () => {
			const sql = this.sql;
			const start = scanStart(query.order, cursor, "ascending");
			const rows = await sql<{ record: SubmissionRecord }[]>`SELECT record FROM submissions
				WHERE store_id = ${this.storeId} AND ${scanCondition(sql, start.after, start.order)}
				${query.conversationId === undefined ? sql`` : sql`AND conversation_id = ${query.conversationId}`}
				${query.status === undefined ? sql`` : sql`AND status = ${query.status}`}
				ORDER BY id ${direction(sql, start.order)} LIMIT ${limit + 1}`;
			return page(
				rows.map((row) => row.record),
				limit,
				start.order,
			);
		});
	}

	submissionByRequest(
		conversationId: ConversationId,
		requestId: string,
		_context: Context,
	): Promise<SubmissionRecord | undefined> {
		return this.admit(async () => {
			const encoded = encodeIndexedString(requestId);
			const [row] = await this.sql<{ record: SubmissionRecord }[]>`SELECT record FROM submissions
				WHERE store_id = ${this.storeId} AND conversation_id = ${conversationId} AND md5(request_id) = md5(${encoded}) AND request_id = ${encoded} LIMIT 1`;
			return row?.record;
		});
	}

	findDocument(address: DocumentAddress, at: DocumentPoint, _context: Context): Promise<DocumentRecord | undefined> {
		return this.admit(async () => {
			const sql = this.sql;
			const [row] = await sql<{ record: DocumentRecord }[]>`SELECT record FROM documents
				WHERE store_id = ${this.storeId} AND ${addressCondition(sql, address)} AND ${lifetimeCondition(sql, at)} ORDER BY created_at DESC LIMIT 1`;
			return row?.record;
		});
	}

	document(id: DocumentId, at: DocumentPoint, _context: Context): Promise<StoredDocument | undefined> {
		return this.admit(() => this.materializeDocument(this.sql, id, at));
	}

	scanDocuments(
		query: DocumentQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<DocumentRecord, Cursor>> {
		return this.admit(async () => {
			const sql = this.sql;
			const scope = scopeColumns(query.scope);
			const kind = query.kind === undefined ? undefined : encodeIndexedString(query.kind);
			const rows = await sql<{ record: DocumentRecord }[]>`SELECT record FROM documents
				WHERE store_id = ${this.storeId} AND scope_kind = ${scope.scopeKind} AND owner_id = ${scope.ownerId} AND id > ${cursorId(cursor) ?? -1}
				${kind === undefined ? sql`` : sql`AND md5(kind) = md5(${kind}) AND kind = ${kind}`}
				AND ${lifetimeCondition(sql, query.at)} ORDER BY id LIMIT ${limit + 1}`;
			return page(
				rows.map((row) => row.record),
				limit,
			);
		});
	}

	close(_context: Context): Promise<void> {
		if (this.closing === undefined) {
			this.closed = true;
			this.closing = this.closeDatabase();
		}
		return this.closing;
	}

	private async closeDatabase(): Promise<void> {
		if (this.active > 0)
			await new Promise<void>((resolve) => {
				this.drained = resolve;
			});
		this.release();
		if (this.ownsPool) await this.sql.end();
	}

	/** Track whole operations, including later queries, before releasing the handle or ending its owned pool. */
	private async admit<T>(operation: () => Promise<T>): Promise<T> {
		if (this.closed) throw new Error("PostgresStorage is closed");
		this.active++;
		try {
			return await operation();
		} finally {
			if (--this.active === 0) this.drained?.();
		}
	}

	private async materializeDocument(
		sql: postgres.ISql,
		id: DocumentId,
		at: DocumentPoint,
	): Promise<StoredDocument | undefined> {
		const upper = at === "current" ? Number.MAX_SAFE_INTEGER : at;
		const [base, ...tail] = await sql<DocumentRow[]>`SELECT document.record, revision.seq,
			revision.kind, revision.version, revision.content FROM documents document
			LEFT JOIN document_revisions revision ON revision.store_id = document.store_id AND revision.document_id = document.id
				AND ${lifetimeCondition(sql, at)}
				AND revision.seq >= (SELECT seq FROM document_revisions
					WHERE store_id = document.store_id AND document_id = document.id AND kind = 'base' AND seq <= ${upper}
					ORDER BY seq DESC LIMIT 1)
				AND revision.seq <= ${upper}
			WHERE document.store_id = ${this.storeId} AND document.id = ${id} ORDER BY revision.seq`;
		if (base === undefined) return undefined;
		const record = base.record;
		if (at !== "current" && isCurrentOnly(record))
			throw new StorageRequestError(`Document ${id} does not retain historical content`);
		if (!isAliveAt(record, at)) return undefined;
		if (base.seq === null) throw new Error(`Document ${id} is missing a required base`);
		let value = base.content as JsonObject;
		for (const revision of tail) {
			if (revision.kind !== "delta" || revision.version !== base.version)
				throw new Error(`Document ${id} crosses a stored version boundary without a base`);
			value = apply(value, revision.content as readonly Op[]) as JsonObject;
		}
		return { record, version: Number(base.version), value, deltasSinceBase: tail.length };
	}

	private async claimGlobalIds(
		sql: postgres.ISql,
		writes: readonly StorageWrite[],
	): Promise<Map<Id<string>, TableName>> {
		const requested = writes.flatMap<{ id: Id<string>; record_type: TableName }>((write) => {
			if (write.type === "document.change" || write.type === "document.retire") return [];
			return write.type === "document.create" || write.type === "document.copy"
				? [{ id: write.record.id, record_type: "document" }]
				: [{ id: write.value.id, record_type: write.type }];
		});
		const rows = [...new Map(requested.map((row) => [row.id, { store_id: this.storeId, ...row }])).values()];
		const existing = new Map<number, TableName>();
		for (const batch of batches(rows, 3)) {
			// The SELECT sees pre-insert rows; data-modifying CTEs share its statement snapshot.
			const previous = await sql<{ id: string; record_type: TableName }[]>`WITH claimed AS (
				INSERT INTO record_ids ${sql(batch, "store_id", "id", "record_type")} ON CONFLICT DO NOTHING
			) SELECT id, record_type FROM record_ids WHERE store_id = ${this.storeId}
				AND id = ANY(${sql.array(
					batch.map((row) => row.id),
					20,
				)})`;
			for (const row of previous) existing.set(Number(row.id), row.record_type);
		}
		const claimed = new Map<Id<string>, TableName>();
		for (const { id, record_type: table } of requested) {
			const owner = existing.get(id);
			const earlier = claimed.get(id);
			if (table === "conversation" || table === "entry" || table === "document") {
				if (owner !== undefined) throw new Error(`ID ${id} already belongs to ${owner}`);
				if (earlier !== undefined) throw new Error(`ID ${id} is written more than once`);
			} else {
				if (owner !== undefined && owner !== table) throw new Error(`ID ${id} already belongs to ${owner}`);
				if (earlier !== undefined && earlier !== table) throw new Error(`ID ${id} is written as two record types`);
			}
			claimed.set(id, table);
		}
		return claimed;
	}

	private prepareDocumentActions(writes: readonly StorageWrite[]): Map<DocumentId, DocumentAction> {
		const actions = new Map<DocumentId, DocumentAction>();
		for (const write of writes) {
			if (
				write.type !== "document.create" &&
				write.type !== "document.copy" &&
				write.type !== "document.change" &&
				write.type !== "document.retire"
			) {
				continue;
			}
			const id = write.type === "document.create" || write.type === "document.copy" ? write.record.id : write.id;
			let action = actions.get(id);
			if (action === undefined) {
				action = { retire: false };
				actions.set(id, action);
			}
			switch (write.type) {
				case "document.create":
					if (action.create !== undefined || action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${id} has more than one content command`);
					}
					action.create = write.record;
					action.content = write.content;
					break;
				case "document.copy":
					if (action.create !== undefined || action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${id} has more than one content command`);
					}
					action.create = write.record;
					action.copy = write.source;
					break;
				case "document.change":
					if (action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${id} has more than one content command`);
					}
					action.content = write.content;
					break;
				case "document.retire":
					if (action.retire) throw new Error(`Document ${id} is retired more than once`);
					action.retire = true;
					break;
			}
		}
		return actions;
	}

	private async checkDocumentActions(
		sql: postgres.ISql,
		actions: ReadonlyMap<DocumentId, DocumentAction>,
	): Promise<Map<DocumentId, DocumentMetadata>> {
		const existing = new Map<DocumentId, DocumentMetadata>();
		if (actions.size === 0) return existing;
		const rows = await sql<({ id: string } & DocumentMetadata)[]>`SELECT document.id, document.record,
			(SELECT version FROM document_revisions WHERE store_id = document.store_id AND document_id = document.id
				ORDER BY seq DESC LIMIT 1) AS version
			FROM documents document WHERE store_id = ${this.storeId} AND id = ANY(${sql.array([...actions.keys()], 20)})`;
		const liveCounts = new Map<string, number>();
		for (const row of rows) {
			existing.set(idFromNumber<DocumentId>(Number(row.id)), row);
			if (row.record.retiredAt === undefined) liveCounts.set(addressKey(row.record), 1);
		}
		const addresses = new Map<string, ReturnType<typeof addressParts>>();
		for (const [id, action] of actions) {
			if (action.copy !== undefined && actions.has(action.copy.id))
				throw new Error(`Document copy ${id} source is changed in the copy batch`);
			const stored = existing.get(id);
			if (action.create === undefined && stored === undefined) throw new Error(`Unknown document: ${id}`);
			if (action.create !== undefined && stored !== undefined) throw new Error(`Document ${id} already exists`);
			if (stored?.record.retiredAt !== undefined) throw new Error(`Document ${id} is retired`);
			if (action.content?.kind === "delta") {
				if (stored?.version == null) throw new Error(`Document ${id} delta has no base`);
				if (Number(stored.version) !== action.content.version)
					throw new Error(`Document ${id} version transition requires a base`);
			}
			if (action.create !== undefined && !action.retire) {
				const key = addressKey(action.create);
				if (!liveCounts.has(key)) addresses.set(key, addressParts(action.create));
			}
		}
		if (addresses.size) {
			const parts = [...addresses.values()];
			const current = await sql<{ record: DocumentRecord }[]>`SELECT current.record FROM unnest(
				${sql.array(
					parts.map((p) => p.kind),
					25,
				)}, ${sql.array(
					parts.map((p) => p.scopeKind),
					25,
				)},
				${sql.array(
					parts.map((p) => p.ownerId),
					20,
				)}, ${sql.array(
					parts.map((p) => p.family),
					20,
				)},
				${sql.array(
					parts.map((p) => p.keyValue),
					25,
				)}
			) requested(kind, scope_kind, owner_id, family, key_value) CROSS JOIN LATERAL (
				SELECT record FROM documents WHERE store_id = ${this.storeId} AND retired_at IS NULL
				AND md5(kind) = md5(requested.kind) AND kind = requested.kind AND scope_kind = requested.scope_kind
				AND owner_id = requested.owner_id AND family = requested.family
				AND md5(key_value) = md5(requested.key_value) AND key_value = requested.key_value LIMIT 1
			) current`;
			for (const row of current) liveCounts.set(addressKey(row.record), 1);
		}
		for (const [id, action] of actions) {
			const key = addressKey(action.create ?? existing.get(id)!.record);
			let live = liveCounts.get(key) ?? 0;
			if (action.retire && action.create === undefined) live--;
			if (action.create !== undefined && !action.retire) live++;
			liveCounts.set(key, live);
		}
		for (const live of liveCounts.values())
			if (live > 1) throw new Error("Document address already has a current incarnation");
		return existing;
	}

	private async applyTableWrites(sql: postgres.ISql, writes: readonly StorageWrite[], seq: Seq): Promise<void> {
		// Full replacements may repeat in a batch. Collapse only after validating every original ID claim.
		const conversations = new Map<ConversationId, ConversationRecord>();
		const entries = new Map<EntryId, EntryRecord>();
		const tasks = new Map<TaskId, StoredTask>();
		const submissions = new Map<SubmissionId, SubmissionRecord>();
		for (const write of writes) {
			switch (write.type) {
				case "conversation":
					conversations.set(write.value.id, write.value);
					break;
				case "entry":
					entries.set(write.value.id, write.value);
					break;
				case "task":
					tasks.set(write.value.id, write.value);
					break;
				case "submission":
					submissions.set(write.value.id, write.value);
					break;
			}
		}
		for (const batch of batches([...conversations.values()], 5)) {
			const rows = batch.map((record) => ({
				store_id: this.storeId,
				id: record.id,
				owner_conversation_id: record.owner?.conversationId ?? null,
				owner_task_id: record.owner?.taskId ?? null,
				record: sql.typed(record, 114),
			}));
			await sql`INSERT INTO conversations ${sql(rows, "store_id", "id", "owner_conversation_id", "owner_task_id", "record")}`;
		}
		for (const batch of batches([...entries.values()], 6)) {
			const rows = batch.map((record) => ({
				store_id: this.storeId,
				id: record.id,
				conversation_id: record.conversationId,
				head: record.head ?? null,
				commit_seq: seq,
				record: sql.typed(record, 114),
			}));
			await sql`INSERT INTO entries ${sql(rows, "store_id", "id", "conversation_id", "head", "commit_seq", "record")}`;
		}
		for (const batch of batches([...tasks.values()], 8)) {
			const rows = batch.map((record) => ({
				store_id: this.storeId,
				id: record.id,
				conversation_id: record.conversationId,
				kind: encodeIndexedString(record.kind),
				status: record.state.status,
				abort_requested: record.abortRequested,
				background: record.background,
				record: sql.typed(record, 114),
			}));
			await sql`INSERT INTO tasks ${sql(rows, "store_id", "id", "conversation_id", "kind", "status", "abort_requested", "background", "record")}
				ON CONFLICT(store_id, id) DO UPDATE SET conversation_id = excluded.conversation_id, kind = excluded.kind,
				status = excluded.status, abort_requested = excluded.abort_requested, background = excluded.background, record = excluded.record`;
		}
		for (const batch of batches([...submissions.values()], 6)) {
			const rows = batch.map((record) => ({
				store_id: this.storeId,
				id: record.id,
				conversation_id: record.conversationId,
				request_id: record.requestId === undefined ? null : encodeIndexedString(record.requestId),
				status: record.status,
				record: sql.typed(record, 114),
			}));
			await sql`INSERT INTO submissions ${sql(rows, "store_id", "id", "conversation_id", "request_id", "status", "record")}
				ON CONFLICT(store_id, id) DO UPDATE SET conversation_id = excluded.conversation_id,
				request_id = excluded.request_id, status = excluded.status, record = excluded.record`;
		}
	}

	private async applyDocumentActions(
		sql: postgres.ISql,
		actions: ReadonlyMap<DocumentId, DocumentAction>,
		existing: ReadonlyMap<DocumentId, DocumentMetadata>,
		seq: Seq,
	): Promise<void> {
		const documents: DocumentRecord[] = [];
		const revisions = new Map<DocumentId, DocumentContent>();
		const prune = new Set<DocumentId>();
		for (const [id, action] of actions) {
			let content = action.content;
			if (action.copy !== undefined) {
				try {
					const stored = await this.materializeDocument(sql, action.copy.id, action.copy.at);
					if (stored === undefined) throw new Error(`Fork source document ${action.copy.id} cannot be read`);
					const create = action.create!;
					if (
						stored.record.scope.kind !== "conversation" ||
						create.scope.kind !== "conversation" ||
						stored.record.kind !== create.kind ||
						stored.record.key !== create.key ||
						stored.record.history !== create.history ||
						stored.record.fork !== create.fork
					)
						throw new Error(`Fork source document ${action.copy.id} does not match the copied record`);
					content = { kind: "base", version: stored.version, value: stored.value };
				} catch (error) {
					throw new Error(`Document copy ${id} was rejected`, { cause: error });
				}
			}
			let record: DocumentRecord =
				action.create === undefined ? existing.get(id)!.record : { ...action.create, createdAt: seq };
			if (action.retire) record = { ...record, retiredAt: seq };
			if (action.create !== undefined || action.retire) documents.push(record);
			if (isCurrentOnly(record) && (action.retire || content?.kind === "base")) prune.add(id);
			if (content !== undefined && !(action.retire && isCurrentOnly(record))) revisions.set(id, content);
		}
		for (const batch of batches(documents, 10)) {
			const rows = batch.map((record) => {
				const parts = addressParts(record);
				return {
					store_id: this.storeId,
					id: record.id,
					kind: parts.kind,
					family: parts.family,
					key_value: parts.keyValue,
					scope_kind: parts.scopeKind,
					owner_id: parts.ownerId,
					created_at: record.createdAt,
					retired_at: record.retiredAt ?? null,
					record: sql.typed(record, 114),
				};
			});
			await sql`INSERT INTO documents ${sql(rows, "store_id", "id", "kind", "family", "key_value", "scope_kind", "owner_id", "created_at", "retired_at", "record")}
				ON CONFLICT (store_id, id) DO UPDATE SET retired_at = excluded.retired_at, record = excluded.record`;
		}
		if (prune.size)
			await sql`DELETE FROM document_revisions WHERE store_id = ${this.storeId} AND document_id = ANY(${sql.array([...prune], 20)})`;
		for (const batch of batches([...revisions], 6)) {
			const rows = batch.map(([id, content]) => ({
				store_id: this.storeId,
				document_id: id,
				seq,
				kind: content.kind,
				version: content.version,
				content: sql.typed(content.kind === "base" ? content.value : content.ops, 114),
			}));
			await sql`INSERT INTO document_revisions ${sql(rows, "store_id", "document_id", "seq", "kind", "version", "content")}`;
		}
	}
}

// Postgres.js accepts at most 65,534 parameters per query.
function* batches<T>(values: readonly T[], columns: number): Generator<readonly T[]> {
	const size = Math.floor(65534 / columns);
	for (let offset = 0; offset < values.length; offset += size) yield values.slice(offset, offset + size);
}

const direction = (sql: postgres.ISql, order: ScanOrder) => (order === "ascending" ? sql`ASC` : sql`DESC`);
const scanCondition = (sql: postgres.ISql, after: number | undefined, order: ScanOrder) =>
	after === undefined ? sql`TRUE` : order === "ascending" ? sql`id > ${after}` : sql`id < ${after}`;
const lifetimeCondition = (sql: postgres.ISql, at: DocumentPoint) =>
	at === "current" ? sql`retired_at IS NULL` : sql`created_at <= ${at} AND (retired_at IS NULL OR retired_at > ${at})`;

function addressCondition(sql: postgres.ISql, address: DocumentAddress | DocumentCreate | DocumentRecord) {
	const parts = addressParts(address);
	return sql`md5(kind) = md5(${parts.kind}) AND kind = ${parts.kind} AND scope_kind = ${parts.scopeKind}
		AND owner_id = ${parts.ownerId} AND family = ${parts.family}
		AND md5(key_value) = md5(${parts.keyValue}) AND key_value = ${parts.keyValue}`;
}
