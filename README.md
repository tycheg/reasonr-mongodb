# ReasonR MongoDB adapters

The ReasonR engine adapters on MongoDB: the main store, asynchronous projections, timers, the outbox, external requests waiting to be sent, idempotency keys, personal data keys, effect stream coordination, the event store, the forget log, feature flag rollout and the schema. A replica set is required: a unit of work commits in a transaction.

The standard `reasonrd` distribution ([reasonr-engine](https://github.com/tycheg/reasonr-engine)) includes the adapters. A server of your own imports the package:

```go
import _ "github.com/tycheg/reasonr-mongodb/src" // stores: { type: mongodb }
```

## Contracts

It requires the contract of plugins alone (`reasonr-engine/port`): imports `port` and `port/registry`; tests: `port/porttest` (the byte-level conformance suites) and the store's own checks. The runs on the example models through the engine are the distribution's (`reasonr-dist`, `integration/mongodb`). The stores implement the byte-level store ports (`port.RowStore` and the others of `port/rows.go`): the engine lays the model out, encrypts personal fields and computes the key columns; the adapter writes, reads and searches by them.

## Layout

| Path | What is in it |
|---|---|
| `src/store.go`, `src/ddl.go` | The store and its collections and indexes by the engine's layout |
| `src/commit.go`, `src/outbox.go` | A unit of work in a transaction, outbox positions |
| `src/effects.go`, `src/events.go`, `src/requests.go` | Timers, the event store, external requests |
| `src/keys.go`, `src/personal.go`, `src/reseal.go`, `src/forgetlog.go` | Personal data keys, encrypted fields, their migrations, the forget log |
| `src/features.go`, `src/register.go` | Feature flags; the `stores` factory |
| `docs` | The documentation |

## Documentation

- [Configuration](docs/configuration.md)
- [Schema](docs/schema.md)
- [Development and tests](docs/development.md)
