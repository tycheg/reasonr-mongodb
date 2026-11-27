# Schema

- A record document: `_id {t: tenant, id: key}`, `tenant`, `id`, `version`, `state` (canonical JSON as a string) and `k_…` fields for index keys. The key types give MongoDB the language's order: integers as int64, decimals and timestamps as Decimal128, strings as UTF-8, uuids as 16 binary bytes; `none` is a missing field. Composite keys are binary and support equality only: a search ordered by them runs in the engine.
- Unique indexes are partial (a key with `none` takes no part). Before a unique index is created, duplicates are checked, and the error lists them for a manual migration (`reasonrd migrate --run`). When the schema changes, the key fields of the model's records are recomputed (renamed fields through `was`).
- Outbox positions come from a counter document updated outside the transaction; positions still in progress are tracked in it, so the reader never skips a position that may still commit.
- Personal fields (RFC §20) are stored encrypted with subject keys in the `sealed` field, index keys on them are blind values; when a field becomes or stops being personal, the documents are re-encrypted after the new schema version.
