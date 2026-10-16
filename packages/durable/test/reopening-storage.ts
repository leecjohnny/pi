import type { Context } from "@earendil-works/chord";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";
import { idFromNumber } from "../src/ids.ts";
import type { ConversationId, EntryId, Id, Seq, Storage, StorageWrite } from "../src/types.ts";

export class ReopeningStorage implements Storage {
	private current: Storage;
	private readonly reopen: () => Promise<Storage>;
	private closed = false;

	constructor(current: Storage, reopen: () => Promise<Storage>) {
		this.current = current;
		this.reopen = reopen;
	}

	async commit(writes: readonly StorageWrite[], commitContext: Parameters<Storage["commit"]>[1]): Promise<Seq> {
		if (this.closed) return this.current.commit(writes, commitContext);
		try {
			return await this.current.commit(writes, commitContext);
		} finally {
			await this.current.close(context);
			this.current = await this.reopen();
		}
	}

	mintId<I extends Id<string>>(): Promise<I> {
		return this.current.mintId<I>();
	}
	conversation: Storage["conversation"] = (id, readContext) => this.current.conversation(id, readContext);
	scanConversations: Storage["scanConversations"] = (query, limit, cursor, readContext) =>
		this.current.scanConversations(query, limit, cursor, readContext);
	entry(id: EntryId, readContext: Context): ReturnType<Storage["entry"]>;
	entry(conversationId: ConversationId, id: EntryId, readContext: Context): ReturnType<Storage["entry"]>;
	entry(idOrConversationId: EntryId | ConversationId, idOrContext: EntryId | Context, readContext?: Context) {
		if (readContext === undefined) {
			return this.current.entry(idFromNumber<EntryId>(idOrConversationId), idOrContext as Context);
		}
		if (typeof idOrContext !== "number") throw new TypeError("Storage.entry() requires an entry ID");
		return this.current.entry(
			idFromNumber<ConversationId>(idOrConversationId),
			idFromNumber<EntryId>(idOrContext),
			readContext,
		);
	}
	findLatestHeadMarker: Storage["findLatestHeadMarker"] = (conversationId, at, readContext) =>
		this.current.findLatestHeadMarker(conversationId, at, readContext);
	scanEntries: Storage["scanEntries"] = (query, limit, cursor, readContext) =>
		this.current.scanEntries(query, limit, cursor, readContext);
	task: Storage["task"] = (id, readContext) => this.current.task(id, readContext);
	scanTasks: Storage["scanTasks"] = (query, limit, cursor, readContext) =>
		this.current.scanTasks(query, limit, cursor, readContext);
	submission: Storage["submission"] = (id, readContext) => this.current.submission(id, readContext);
	scanSubmissions: Storage["scanSubmissions"] = (query, limit, cursor, readContext) =>
		this.current.scanSubmissions(query, limit, cursor, readContext);
	submissionByRequest: Storage["submissionByRequest"] = (conversationId, requestId, readContext) =>
		this.current.submissionByRequest(conversationId, requestId, readContext);
	findDocument: Storage["findDocument"] = (address, at, readContext) =>
		this.current.findDocument(address, at, readContext);
	document: Storage["document"] = (id, at, readContext) => this.current.document(id, at, readContext);
	scanDocuments: Storage["scanDocuments"] = (query, limit, cursor, readContext) =>
		this.current.scanDocuments(query, limit, cursor, readContext);

	async close(closeContext: Parameters<Storage["close"]>[0]): Promise<void> {
		if (this.closed) return;
		this.closed = true;
		await this.current.close(closeContext);
	}
}
