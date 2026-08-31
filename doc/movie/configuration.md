# Movie Configuration

[Documentation index](README.md) · [Development workflow](development_workflow.md) · [Recovery backlog](backlog.md)

Movie configuration can be built programmatically or loaded from YAML/JSON. `ActorSystem.new(behavior, config)` merges the supplied values over `ActorSystemConfig.default`.

## Value and path semantics

`ConfigValue` supports strings, integers, floats, booleans, arrays, nested objects, and explicit `null`.

- `has_path?` distinguishes an explicit `null` from a missing path.
- `config[path]` returns the stored value, including `nil`, and raises `MissingConfigError` only when the path is absent.
- `config[path]?` returns `nil` for both explicit `null` and absence; use `has_path?` when the distinction matters.
- A typed getter with a default uses the default only for an absent path. Explicit `null` is present and raises `WrongTypeConfigError` for non-null typed getters.
- Invalid numeric, boolean, and duration strings raise `WrongTypeConfigError`; raw parser exceptions are not part of the public contract.

## Loading and composition

```crystal
defaults = Movie::ActorSystemConfig.default
file = Movie::Config.load("movie.yml", defaults)
config = file.with_env_overrides
```

- `with_fallback(other)`: the receiver wins; missing values come from `other`.
- `with_override(other)`: `other` wins.
- Nested objects are merged recursively; scalar and array values are replaced.

## Supported schema

| Path | Type | Default | Meaning |
|---|---|---:|---|
| `name` | String | empty | Actor-system name; empty generates a unique name. |
| `root.restart-strategy` | String | `restart` | Root actor behavior after its own failure: `restart` or `stop`. |
| `supervision.strategy` | String | `restart` | Child failure action: `restart`, `stop`, `resume`, or `escalate`. |
| `supervision.scope` | String | `one-for-one` | Apply the action to one child or all children: `one-for-one`, `all-for-one`. |
| `supervision.max-restarts` | Int | `3` | Maximum attempts in the configured window. |
| `supervision.within` | Duration | `1s` | Restart accounting window. |
| `supervision.backoff.min` | Duration | `10ms` | First restart delay. |
| `supervision.backoff.max` | Duration | `1s` | Maximum restart delay. |
| `supervision.backoff.factor` | Float | `2.0` | Exponential backoff multiplier. |
| `supervision.backoff.jitter` | Float | `0.0` | Random delay factor. |
| `remoting.enabled` | Bool | `false` | Start the experimental TCP remoting extension automatically. |
| `remoting.host` | String | `127.0.0.1` | Remoting bind host. |
| `remoting.port` | Int | `2552` | Remoting bind port; `0` selects a free port. |
| `remoting.stripe-count` | Int | `8` | Parallel outbound TCP connections per remote address. |
| `executor.pool-size` | Int | `4` | Bounded executor worker count. |
| `executor.queue-capacity` | Int | `128` | Number of queued executor tasks. |
| `persistence.backend` | String | `sqlite` | Registered persistence backend; `postgres` requires the PostgreSQL entrypoint. |
| `persistence.connection-uri` | String | empty | Required for PostgreSQL; credentials and TLS parameters belong in this URI. |
| `persistence.db-path` | String | `data/movie_persistence.sqlite3` | SQLite database path; missing parent directories are created on startup. |
| `persistence.pool-size` | Int | `1` | Backend connection worker count; values below one are clamped to one. |
| `persistence.io-queue-capacity` | Int | `256` | Bounded pending jobs per isolated backend connection worker. |
| `persistence.operation-timeout` | Duration | `5s` | Ask timeout for journal and durable-state operations; timeout does not cancel backend work already running. |
| `persistence.retry.max-retries` | Int | `2` | Automatic retries after connection loss for typed idempotent requests. |
| `persistence.retry.min-backoff` | Duration | `10ms` | Delay before the first automatic connection retry. |
| `persistence.retry.max-backoff` | Duration | `250ms` | Upper bound for exponential connection-retry delay. |
| `persistence.circuit-breaker.failure-threshold` | Int | `5` | Consecutive connection failures per worker before its circuit opens. |
| `persistence.circuit-breaker.reset-timeout` | Duration | `5s` | Delay before an open worker circuit permits a probe request. |

The old `movie.persistence.db_path` and `movie.persistence.pool_size` shapes are not aliases. Movie is still pre-1.0, and configuration now has one canonical naming scheme.

## Environment overrides

The default prefix is `MOVIE`. Canonical environment names use `__` between dotted path segments and `_` for a hyphen inside a segment:

| Environment variable | Configuration path |
|---|---|
| `MOVIE_NAME` | `name` |
| `MOVIE_ROOT__RESTART_STRATEGY` | `root.restart-strategy` |
| `MOVIE_SUPERVISION__MAX_RESTARTS` | `supervision.max-restarts` |
| `MOVIE_SUPERVISION__BACKOFF__MIN` | `supervision.backoff.min` |
| `MOVIE_REMOTING__STRIPE_COUNT` | `remoting.stripe-count` |
| `MOVIE_EXECUTOR__QUEUE_CAPACITY` | `executor.queue-capacity` |
| `MOVIE_PERSISTENCE__BACKEND` | `persistence.backend` |
| `MOVIE_PERSISTENCE__CONNECTION_URI` | `persistence.connection-uri` |
| `MOVIE_PERSISTENCE__DB_PATH` | `persistence.db-path` |
| `MOVIE_PERSISTENCE__IO_QUEUE_CAPACITY` | `persistence.io-queue-capacity` |
| `MOVIE_PERSISTENCE__OPERATION_TIMEOUT` | `persistence.operation-timeout` |
| `MOVIE_PERSISTENCE__RETRY__MAX_RETRIES` | `persistence.retry.max-retries` |
| `MOVIE_PERSISTENCE__RETRY__MIN_BACKOFF` | `persistence.retry.min-backoff` |
| `MOVIE_PERSISTENCE__RETRY__MAX_BACKOFF` | `persistence.retry.max-backoff` |
| `MOVIE_PERSISTENCE__CIRCUIT_BREAKER__FAILURE_THRESHOLD` | `persistence.circuit-breaker.failure-threshold` |
| `MOVIE_PERSISTENCE__CIRCUIT_BREAKER__RESET_TIMEOUT` | `persistence.circuit-breaker.reset-timeout` |

Legacy single-underscore names remain accepted for simple paths where every underscore means a dot, such as `MOVIE_REMOTING_PORT` -> `remoting.port`. Use the canonical double-underscore form whenever a path segment contains a hyphen.

When a path already exists in the base configuration, its schema type controls conversion. This keeps values such as `MOVIE_REMOTING__PORT=0` numeric while still accepting `0`/`1` for a known boolean path. Invalid or overflowing typed values raise `WrongTypeConfigError` while applying overrides.

For new paths without a schema value, conversion uses unambiguous boolean words (`true`/`false`, `yes`/`no`, `on`/`off`), integers (including `0` and `1`), floats, comma-separated string arrays, then strings.

## Feature entrypoints

Core configuration, executor, streams, and remoting are available through `require "movie"`. SQLite persistence is optional:

```crystal
require "movie"
require "movie/persistence"
```

PostgreSQL persistence includes the core persistence API and registers the `postgres` backend:

```crystal
require "movie/persistence/postgres"
```
