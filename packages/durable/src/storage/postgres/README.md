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

Each commit is one transaction. It validates all original global ID claims and
document commands before applying the batch. ID claims and table writes use
bounded multi-row inserts. Repeated complete task or submission replacements
retain the last value after validation. Metadata advances in the same transaction.
`mintId()` separately advances the durable counter with `UPDATE ... RETURNING`,
so unused IDs remain consumed after reopen or a failed commit. This costs one
database command per allocation. There is no commit retry or cross-process lock.

History reads first load immutable ancestor IDs and cumulative fork cutoffs with
a recursive CTE. A second query uses concrete bounds in separately limited
`UNION ALL` branches. Each branch can use the conversation index even when most
entries belong to other conversations. Pages retain native ancestry order and
cursor semantics. Head queries return the marker, as the Storage contract requires.

Document reads use a read-only repeatable-read transaction for the incarnation,
base, and delta tail. Concurrent base replacement or retirement cannot tear those
reads. Copies use the enclosing commit transaction and the committed source.
Current-only documents reclaim old revisions at a base or retirement; rewindable
documents retain their history.

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
