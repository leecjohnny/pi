# PostgreSQL storage

`PostgresStorage` implements the native `Storage` interface with Postgres.js.
Session and Harness use the same records, document revisions, and query contracts
as the SQLite backend. See the [package guide](../../../README.md#storage) for
the public import and an example.

Both modes use one schema and the same queries. `PostgresStorage.open(sql)`
opens the `default` store and owns the client. For several users or sessions,
`PostgresStorage.openBackend(sql)` migrates once and owns a shared pool.
`backend.openStore(storeId)` binds an immutable ID to a Storage handle before
`Harness.open()`. Each native primary key, index, read, write, ID counter, and
commit sequence is scoped by `store_id`. Native JSON records remain unchanged.
A data-modifying CTE initializes a store or reads its existing counters in one
command. It leaves existing metadata unchanged and detects missing counters.
Backend open reads the schema version first; an already current schema needs
only reads, with no DDL transaction or schema-write locks.
The application chooses stable store IDs, authorizes access, and ensures one
owning Session per store. Several sessions for one user can use separate IDs.

Closing a scoped handle rejects new operations and drains its admitted work,
without closing the shared pool. Closing the backend closes all handles before
ending the pool. The default store uses the same engine; its close also ends
the pool. Open handles only after migrations, and migrate an inactive backend.
New stores start at native ID 2 and sequence 1. Opening an existing store retains
its counters; missing counters with persisted records are an error.

The schema follows SQLite's native tables: global ID ownership, conversations,
entries, complete task and submission records, document incarnations, and base
and delta revisions. IDs, sequences, and document versions use `BIGINT`; known
native numeric results are converted at the Storage boundary. Full records and
revision content use PostgreSQL `json`, bound with `sql.typed(value, 114)`.
`sql.json()` binds JSONB, which rejects lone UTF-16 surrogates and escaped NUL
that the native JSON contract preserves.

Indexed string identities use JSON encoding to preserve every JavaScript string.
Their text columns use `COLLATE "C"`. Unbounded kinds, keys, and request IDs use
`md5()` expression indexes so long identities cannot exceed B-tree entry limits.
Queries always check full encoded equality too; the digest only selects candidates.

Each commit is one transaction. A [data-modifying CTE](https://www.postgresql.org/docs/18/queries-with.html#QUERIES-WITH-MODIFYING) reserves global IDs and
reads their previous owners from the same statement snapshot. Validation checks
every original claim; a failure rolls back the entire transaction. This combines
the ownership lookup and insert without rewriting existing claim rows.
Validation loads document metadata and current versions in one query. A new
document address uses an indexed lookup through
`unnest` and `LATERAL`; changes to existing content reuse the metadata already
read. ID claims, document records and revisions use bounded multi-row writes.
Current-only revision cleanup uses one array-bound delete. Complete task and
submission replacements retain the last value after validating every original
ID claim. `UPDATE ... RETURNING` advances metadata atomically with the writes.
`mintId()` separately advances the durable counter with `UPDATE ... RETURNING`,
so unused IDs remain consumed after reopen or a failed commit. The host still
provides one Session writer per store; this is not a distributed writer lease.

History reads use one recursive CTE for immutable ancestor IDs and cumulative
fork cutoffs. A limited `LATERAL` index scan reads each ancestor. The final sort
and limit retain native ancestry order, page bounds and cursor semantics without
sending ancestry to the application or building a query per branch. Scoped entry
and head lookups use the same lineage. Every lookup includes `store_id`.

A document read is one SQL statement joining its incarnation to the latest base
and bounded revision tail. PostgreSQL's [statement snapshot](https://www.postgresql.org/docs/18/transaction-iso.html#XACT-READ-COMMITTED) keeps the read
consistent with concurrent replacement or retirement, so a separate read-only
transaction is unnecessary. Delta replay remains in Chord and preserves native
JSON values. Copies use the enclosing commit transaction and the committed
source. Current-only documents reclaim old revisions at a base or retirement;
rewindable documents retain their history. Schema version 2 adds a partial index
on `(store_id, scope_kind, owner_id, id)` for non-retired documents. Current scans
then skip retired incarnations, while historical scans retain their full indexes.
The migration adds an index and preserves native records and counters.

The PostgreSQL tests require `PI_DURABLE_POSTGRES_URL` pointing to a disposable
local database where the test user can create schemas. Each case creates and
drops its own schema. From the repository root, run:

```sh
PI_DURABLE_POSTGRES_URL=postgres://postgres:postgres@localhost:5432/pi_storage_test ./test.sh
```

The suite runs shared conformance in default and shared-store modes, normally
and with reopen after every commit. Shared cases run two stores with colliding
native IDs in the same tables. Harness cases verify isolation and reopen,
plus SQLite-equivalent persistence, corruption, document reclamation, and atomic
migration tests. PostgreSQL-specific cases cover durable unused IDs, parameter
limits, repeated upserts, large identities, consistent reads, and close draining.
