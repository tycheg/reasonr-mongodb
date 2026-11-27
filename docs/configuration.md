# Configuration

```yaml
stores:
  main: { type: mongodb, uri_env: REASONR_MONGODB_URI, database: reasonr }

state: main
events: main
```

| Key | What it sets |
|---|---|
| `uri`, `uri_env` | The connection string (with `replicaSet`), or the environment variable that holds it |
| `database` | The database; `reasonr` by default |

MongoDB has no notification hub of its own: the `memory` hub delivers only within one process, so with several instances serving clients use the `postgres` hub with its own `dsn`.
