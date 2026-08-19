# Configuration

Movie configuration is a typed, path-based tree loaded from YAML, JSON, or the fluent `ConfigBuilder` API.

```crystal
config = Movie::Config.load("movie.yml", Movie::ActorSystemConfig.default)
system = Movie::ActorSystem.new(main_behavior, config)
```

## Value Semantics

Supported stored values are strings, integers, floats, booleans, arrays, and nested objects. Explicit `null` values are not supported and are rejected by YAML and JSON loaders with `Movie::ConfigError`.

Missing paths are different from stored values:

- `config["path"]` and typed accessors raise `Movie::MissingConfigError` when the path is missing.
- `config["path"]?`, `get_value`, and typed accessors with a default return the fallback for a missing path.
- Wrong value types and invalid conversions raise `Movie::WrongTypeConfigError`.
- Malformed YAML or JSON raises `Movie::ConfigError`.

Configuration paths use dots between nested keys. Key segments use lower-case kebab-case.

## Actor System

| Key | Default | Values / meaning |
| --- | --- | --- |
| `name` | generated | Actor system name. An empty value generates a name. |
| `actor.restart-strategy` | `restart` | Root actor behavior: `restart` or `stop`. |
| `supervision.strategy` | `restart` | Child failure action: `restart`, `stop`, `resume`, or `escalate`. |
| `supervision.scope` | `one-for-one` | `one-for-one` or `all-for-one`. |
| `supervision.max-restarts` | `3` | Maximum restarts in the configured window. |
| `supervision.within` | `1s` | Restart counter window. |
| `supervision.backoff.min` | `10ms` | Minimum restart backoff. |
| `supervision.backoff.max` | `1s` | Maximum restart backoff. |
| `supervision.backoff.factor` | `2.0` | Exponential backoff multiplier. |
| `supervision.backoff.jitter` | `0.0` | Random jitter factor from `0.0` to `1.0`. |

`actor.restart-strategy` is intentionally independent from `supervision.strategy`. For example, `resume` is valid for supervision and must not be parsed as a root restart strategy.

## Remoting

| Key | Default | Meaning |
| --- | --- | --- |
| `remoting.enabled` | `false` | Enable remoting during system startup. |
| `remoting.host` | `127.0.0.1` | Bind host. |
| `remoting.port` | `2552` | Bind port. `0` asks the OS to select a free port. |
| `remoting.stripe-count` | `8` | Connection pool stripe count. |

## Executor

The executor extension reads these keys when it is created:

| Key | Default | Meaning |
| --- | --- | --- |
| `executor.pool-size` | `4` | Number of worker fibers. |
| `executor.queue-capacity` | `128` | Bounded task queue capacity. |

## Persistence

| Canonical key | Default | Meaning |
| --- | --- | --- |
| `movie.persistence.db-path` | `data/movie_persistence.sqlite3` | SQLite database path. |
| `movie.persistence.pool-size` | `1` | Number of persistence connection actors. |

The previous underscore spellings `movie.persistence.db_path` and `movie.persistence.pool_size` remain accepted as compatibility aliases. If both spellings are present, the canonical kebab-case key wins.

## YAML Example

```yaml
name: orders
actor:
  restart-strategy: stop
supervision:
  strategy: resume
  scope: one-for-one
  max-restarts: 5
  within: 30s
  backoff:
    min: 50ms
    max: 2s
    factor: 2.0
    jitter: 0.1
remoting:
  enabled: true
  host: 127.0.0.1
  port: 0
  stripe-count: 4
executor:
  pool-size: 4
  queue-capacity: 256
movie:
  persistence:
    db-path: data/orders.sqlite3
    pool-size: 2
```

## Environment Overrides

The canonical environment syntax uses double underscores for path boundaries and a single underscore inside a key segment. Segment underscores become hyphens:

```bash
MOVIE__REMOTING__PORT=0
MOVIE__REMOTING__STRIPE_COUNT=4
MOVIE__ACTOR__RESTART_STRATEGY=stop
MOVIE__MOVIE__PERSISTENCE__DB_PATH=data/orders.sqlite3
```

These map to `remoting.port`, `remoting.stripe-count`, `actor.restart-strategy`, and `movie.persistence.db-path` respectively.

The legacy form remains supported for existing deployments. For example, `MOVIE_REMOTING_PORT` maps to `remoting.port`. When both forms target the same path, the canonical double-underscore form wins.

Environment values are converted as follows:

- `true`, `yes`, and `on` become `true`.
- `false`, `no`, and `off` become `false`.
- `0` and `1` remain integer values, which allows settings such as `remoting.port=0`.
- Integer and decimal strings become numeric values.
- Comma-separated values become string arrays.
- Other values remain strings.

Use `config.with_env_overrides` after loading the base configuration and before passing it to `ActorSystem.new`.
