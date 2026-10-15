import { randomBytes } from "node:crypto";
import type { JsonValue } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { describe, expect, it } from "vitest";
import { idFromNumber } from "../src/ids.ts";
import type {
	ConversationId,
	DocumentCreate,
	DocumentId,
	EntryId,
	EntryRecord,
	Seq,
	Storage,
	StorageWrite,
	SubmissionId,
	SubmissionRecord,
	TaskId,
	TaskRecord,
} from "../src/types.ts";
import { ROOT_CONVERSATION_ID } from "../src/types.ts";
import { createPostgresDatabase } from "./postgres-fixture.ts";
import { ReopeningStorage } from "./reopening-storage.ts";

const context = BACKGROUND_CONTEXT;

async function createPostgresStorage() {
	const database = await createPostgresDatabase();
	return { ...database, storage: await database.open() };
}

registerStorageConformance({ describe, expect, it }, "PostgresStorage", async (use) =>
	use((await createPostgresStorage()).storage),
);
registerStorageConformance({ describe, expect, it }, "PostgresStorage across reopen", async (use) => {
	const database = await createPostgresDatabase();
	const storage = new ReopeningStorage(await database.open(), database.open);
	try {
		await use(storage);
	} finally {
		await storage.close(context);
	}
});

function entry(id: EntryId, conversationId: ConversationId, data?: JsonValue): EntryRecord {
	return { id, conversationId, kind: "message", ...(data === undefined ? {} : { data }) };
}

async function createRoot(storage: Storage): Promise<Seq> {
	return storage.commit([{ type: "conversation", value: { id: ROOT_CONVERSATION_ID } }], context);
}

function pendingTask(id: TaskId<JsonValue>): TaskRecord<JsonValue, JsonValue, JsonValue> {
	return {
		id,
		conversationId: ROOT_CONVERSATION_ID,
		kind: "test.task",
		version: 1,
		input: null,
		state: { status: "pending", checkpoint: { phase: "ready" } },
		background: false,
		abortRequested: false,
	};
}

describe("PostgresStorage persistence and queries", () => {
	it("persists records, sequence allocation, and global ID allocation across reopen", async () => {
		const { storage, open } = await createPostgresStorage();
		expect(await createRoot(storage)).toBe(1);
		const entryId = await storage.mintId<EntryId>();
		expect(await storage.commit([{ type: "entry", value: entry(entryId, ROOT_CONVERSATION_ID) }], context)).toBe(2);
		await storage.close(context);

		const reopened = await open();
		expect(await reopened.entry(entryId, context)).toEqual({
			entry: entry(entryId, ROOT_CONVERSATION_ID),
			commitSeq: 2,
		});
		expect(await reopened.mintId<EntryId>()).toBe(entryId + 1);
		expect(
			await reopened.commit(
				[{ type: "task", value: pendingTask(await reopened.mintId<TaskId<JsonValue>>()) }],
				context,
			),
		).toBe(3);
	});

	it("rejects persisted metadata corruption on reopen", async () => {
		const { storage, sql, open } = await createPostgresStorage();
		await storage.close(context);
		await sql`DELETE FROM durable_metadata`;
		await expect(open()).rejects.toThrow("Durable PostgreSQL metadata is missing");
	});

	it("rejects a document whose required base is missing", async () => {
		const { storage, sql } = await createPostgresStorage();
		await createRoot(storage);
		const id = await storage.mintId<DocumentId>();
		await storage.commit(
			[
				{
					type: "document.create",
					record: { id, kind: "corrupt", scope: { kind: "session" } },
					content: { kind: "base", version: 1, value: { retained: true } },
				},
			],
			context,
		);
		await sql`DELETE FROM document_revisions WHERE document_id = ${id}`;
		await expect(storage.document(id, "current", context)).rejects.toThrow(
			`Document ${id} is missing a required base`,
		);
	});

	it("replays detached root replacements and follow-up edits while rejecting corrupt operations", async () => {
		const { storage, sql } = await createPostgresStorage();
		await createRoot(storage);
		const id = await storage.mintId<DocumentId>();
		await storage.commit(
			[
				{
					type: "document.create",
					record: { id, kind: "replay", scope: { kind: "session" } },
					content: { kind: "base", version: 1, value: { nested: { value: 1 }, rows: [] } },
				},
			],
			context,
		);
		await storage.commit(
			[
				{
					type: "document.change",
					id,
					content: {
						kind: "delta",
						version: 1,
						ops: [["r", { nested: { value: 2 }, rows: [{ id: 1 }] }]],
					},
				},
			],
			context,
		);
		await storage.commit(
			[
				{
					type: "document.change",
					id,
					content: {
						kind: "delta",
						version: 1,
						ops: [
							["s", ["nested", "value"], 3],
							["p", ["rows"], 1, 0, [{ id: 2 }]],
							["m", ["rows"], [1, 0]],
						],
					},
				},
			],
			context,
		);
		await storage.commit(
			[
				{
					type: "document.change",
					id,
					content: { kind: "delta", version: 1, ops: [["s", ["nested", "value"], 4]] },
				},
			],
			context,
		);

		const expected = { nested: { value: 4 }, rows: [{ id: 2 }, { id: 1 }] };
		const first = (await storage.document(id, "current", context))!;
		expect(first.value).toEqual(expected);
		(first.value.nested as { value: number }).value = 99;
		(first.value.rows as Array<{ id: number }>)[0]!.id = 99;
		expect((await storage.document(id, "current", context))?.value).toEqual(expected);

		await sql`UPDATE document_revisions SET content = ${sql.typed([["unknown"]], 114)} WHERE document_id = ${id}
			AND seq = (SELECT max(seq) FROM document_revisions WHERE document_id = ${id})`;

		await expect(storage.document(id, "current", context)).rejects.toThrow("unknown op verb");
	});

	it("rolls SQL rows and sequence allocation back as one transaction", async () => {
		const { storage, sql } = await createPostgresStorage();
		await createRoot(storage);
		const transientId = await storage.mintId<EntryId>();
		const transientTaskId = await storage.mintId<TaskId<JsonValue>>();
		const circular: { self?: unknown } = {};
		circular.self = circular;
		await expect(
			storage.commit(
				[
					{ type: "entry", value: entry(transientId, ROOT_CONVERSATION_ID) },
					{
						type: "task",
						value: { ...pendingTask(transientTaskId), input: circular as unknown as JsonValue },
					},
				],
				context,
			),
		).rejects.toThrow("circular structure");
		expect(await storage.entry(transientId, context)).toBeUndefined();
		expect(await storage.task(transientTaskId, context)).toBeUndefined();
		const committedId = await storage.mintId<EntryId>();
		expect(await storage.commit([{ type: "entry", value: entry(committedId, ROOT_CONVERSATION_ID) }], context)).toBe(
			2,
		);

		expect((await sql`SELECT count(*)::integer AS count FROM entries`)[0]?.count).toBe(1);
		expect((await sql`SELECT next_seq FROM durable_metadata`)[0]?.next_seq).toBe("3");
	});

	it("reconstructs recent and ancient rewindable points after reopen", async () => {
		const { storage, open } = await createPostgresStorage();
		await createRoot(storage);
		const id = await storage.mintId<DocumentId>();
		const record = {
			id,
			kind: "history",
			scope: { kind: "conversation", conversationId: ROOT_CONVERSATION_ID },
			history: "rewindable",
			fork: "asOf",
		} satisfies DocumentCreate;
		const createdAt = await storage.commit(
			[{ type: "document.create", record, content: { kind: "base", version: 1, value: { count: 0 } } }],
			context,
		);
		let ancientAt = createdAt;
		let recentAt = createdAt;
		for (let count = 1; count <= 40; count++) {
			recentAt = await storage.commit(
				[
					{
						type: "document.change",
						id,
						content:
							count === 20
								? { kind: "base", version: 1, value: { count } }
								: { kind: "delta", version: 1, ops: [["s", ["count"], count]] },
					},
				],
				context,
			);
			if (count === 5) ancientAt = recentAt;
		}
		await storage.close(context);
		const reopened = await open();
		expect((await reopened.document(id, ancientAt, context))?.value).toEqual({ count: 5 });
		expect((await reopened.document(id, recentAt, context))?.value).toEqual({ count: 40 });
	});

	it("reclaims current-only revisions only after a base or retirement", async () => {
		const { storage, sql } = await createPostgresStorage();
		await createRoot(storage);
		const id = await storage.mintId<DocumentId>();
		await storage.commit(
			[
				{
					type: "document.create",
					record: { id, kind: "latest", scope: { kind: "session" } },
					content: { kind: "base", version: 1, value: { count: 0 } },
				},
			],
			context,
		);
		for (let count = 1; count <= 10; count++) {
			await storage.commit(
				[{ type: "document.change", id, content: { kind: "delta", version: 1, ops: [["s", ["count"], count]] } }],
				context,
			);
		}
		expect(
			Number((await sql`SELECT count(*) AS count FROM document_revisions WHERE document_id = ${id}`)[0]?.count),
		).toBe(11);
		expect(
			(
				await sql`SELECT content FROM document_revisions WHERE document_id = ${id} AND kind = 'delta' ORDER BY seq DESC LIMIT 1`
			)[0]?.content,
		).toEqual([["s", ["count"], 10]]);
		await storage.commit(
			[{ type: "document.change", id, content: { kind: "base", version: 1, value: { count: 11 } } }],
			context,
		);
		expect(
			Number((await sql`SELECT count(*) AS count FROM document_revisions WHERE document_id = ${id}`)[0]?.count),
		).toBe(1);
		await storage.commit(
			[
				{
					type: "document.change",
					id,
					content: { kind: "delta", version: 1, ops: [["r", { count: 12 }]] },
				},
			],
			context,
		);
		expect(
			Number((await sql`SELECT count(*) AS count FROM document_revisions WHERE document_id = ${id}`)[0]?.count),
		).toBe(2);
		await storage.commit([{ type: "document.retire", id }], context);
		expect(
			Number((await sql`SELECT count(*) AS count FROM document_revisions WHERE document_id = ${id}`)[0]?.count),
		).toBe(0);
	});
	it("durably consumes unused IDs, across reopen and at exhaustion", async () => {
		const { storage, open, sql } = await createPostgresStorage();
		expect(await createRoot(storage)).toBe(1);
		const ids = await Promise.all(Array.from({ length: 20 }, () => storage.mintId<EntryId>()));
		expect([...ids].sort((a, b) => a - b)).toEqual(Array.from({ length: 20 }, (_, i) => i + 2));
		await storage.close(context);
		const reopened = await open();
		expect(await reopened.mintId<EntryId>()).toBe(22);
		await sql`UPDATE durable_metadata SET next_id = ${Number.MAX_SAFE_INTEGER}::bigint`;
		expect(await reopened.mintId<EntryId>()).toBe(Number.MAX_SAFE_INTEGER);
		await expect(reopened.mintId()).rejects.toThrow("ID space is exhausted");
		expect(await reopened.commit([], context)).toBe(2);
		await expect(reopened.mintId()).rejects.toThrow("ID space is exhausted");
	});

	it("chunks large commits and keeps the last task and submission replacements", async () => {
		const { storage } = await createPostgresStorage();
		await createRoot(storage);
		const writes: StorageWrite[] = Array.from({ length: 14000 }, (_, i) => ({
			type: "entry",
			value: entry(idFromNumber<EntryId>(i + 2), ROOT_CONVERSATION_ID),
		}));
		const task = pendingTask(idFromNumber<TaskId<JsonValue>>(14002));
		const submission = {
			id: idFromNumber<SubmissionId>(14003),
			conversationId: ROOT_CONVERSATION_ID,
			type: "input",
			status: "queued",
		} satisfies SubmissionRecord;
		writes.push(
			{ type: "task", value: task },
			{ type: "submission", value: submission },
			{ type: "task", value: { ...task, background: true } },
			{ type: "submission", value: { ...submission, status: "unanswered", reason: "test" } },
		);
		expect(await storage.commit(writes, context)).toBe(2);
		expect(
			(await storage.scanEntries({ conversationId: ROOT_CONVERSATION_ID }, 14001, undefined, context)).items,
		).toHaveLength(14000);
		expect(await storage.task(task.id, context)).toEqual({ ...task, background: true });
		expect(await storage.submission(submission.id, context)).toEqual({
			...submission,
			status: "unanswered",
			reason: "test",
		});
		expect(await storage.mintId()).toBe(14004);
	});

	it("stores and looks up incompressible long identities without B-tree size limits", async () => {
		const { storage } = await createPostgresStorage();
		await createRoot(storage);
		const key = `${randomBytes(8192).toString("base64")}\u0000\ud800`;
		const task = { ...pendingTask(await storage.mintId<TaskId<JsonValue>>()), kind: key };
		const submission = {
			id: await storage.mintId<SubmissionId>(),
			conversationId: ROOT_CONVERSATION_ID,
			type: "input",
			status: "queued",
			requestId: key,
		} satisfies SubmissionRecord;
		const record: DocumentCreate = {
			id: await storage.mintId<DocumentId>(),
			scope: { kind: "session" },
			kind: key,
			key,
		};
		await storage.commit(
			[
				{ type: "task", value: task },
				{ type: "submission", value: submission },
				{ type: "document.create", record, content: { kind: "base", version: 1, value: { text: key } } },
			],
			context,
		);
		expect((await storage.scanTasks({ kind: key }, 10, undefined, context)).items).toEqual([task]);
		expect(await storage.submissionByRequest(ROOT_CONVERSATION_ID, key, context)).toEqual(submission);
		expect(await storage.findDocument(record, "current", context)).toMatchObject(record);
		expect(
			(await storage.scanDocuments({ scope: record.scope, kind: key, at: "current" }, 10, undefined, context)).items,
		).toHaveLength(1);
		expect((await storage.document(record.id, "current", context))?.value).toEqual({ text: key });
	});

	it("keeps a consistent document snapshot during retirement and drains its read before close", async () => {
		const { storage, sql, schema, open } = await createPostgresStorage();
		await createRoot(storage);
		const id = await storage.mintId<DocumentId>();
		await storage.commit(
			[
				{
					type: "document.create",
					record: { id, kind: "snapshot", scope: { kind: "session" } },
					content: { kind: "base", version: 1, value: { retained: true } },
				},
			],
			context,
		);
		const writer = await sql.reserve();
		try {
			await writer`BEGIN`;
			await writer`LOCK TABLE document_revisions IN ACCESS EXCLUSIVE MODE`;
			const read = storage.document(id, "current", context);
			await expect
				.poll(
					async () =>
						(
							await sql`SELECT count(*)::integer AS count FROM pg_stat_activity
				WHERE application_name = ${schema} AND wait_event_type = 'Lock' AND query LIKE '%FROM document_revisions%'`
						)[0]?.count,
				)
				.toBe(1);
			const closing = storage.close(context);
			await expect(storage.document(id, "current", context)).rejects.toThrow("is closed");
			await writer`UPDATE documents SET retired_at = 3, record = ${writer.typed({ id, kind: "snapshot", scope: { kind: "session" }, createdAt: 2, retiredAt: 3 }, 114)} WHERE id = ${id}`;
			await writer`DELETE FROM document_revisions WHERE document_id = ${id}`;
			await writer`COMMIT`;
			expect((await read)?.value).toEqual({ retained: true });
			await closing;
			expect(await (await open()).document(id, "current", context)).toBeUndefined();
		} finally {
			await writer`ROLLBACK`;
			writer.release();
		}
	});
});
