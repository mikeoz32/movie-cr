# Configuration

Movie configuration is path-based. The canonical naming convention is lowercase dotted segments: `feature.subsystem.setting`. Hyphens and underscores are not used in canonical paths.

## Loading And Precedence

`ActorSystem` combines a caller-provided `Config` with `ActorSystemConfig.default`. Caller-provided values override defaults. Environment variables can be applied last with `Config#with_env_overrides`, so they override both file and default values.

```crystal
config = Movie::Config.load_yaml("movie.yml")
  .with_env_overrides

system = Movie::ActorSystem(Symbol).new(
  Movie::Behaviors(Symbol).same,
  config
)
```

Environment variables use the `MOVIE_` prefix. The remaining uppercase name is lowercased and every underscore becomes a path separator:

```text
MOVIE_REMOTING_PORT=0       -> remoting.port = 0
MOVIE_EXECUTOR_POOL_SIZE=8  -> executor.pool.size = 8
MOVIE_PERSISTENCE_DB_PATH=app.sqlite3 -> persistence.db.path = "app.sqlite3"
```

Values are converted to booleans, integers, floats, comma-separated string arrays, or strings. Pass a custom prefix to `with_env_overrides` when the deployment needs one.

## Supported Keys

### System And Actors

| Key | Default | Meaning |
| --- | --- | --- |
| `name` | generated | Actor system name. An empty value requests an auto-generated name. |
| `actor.restart.strategy` | `restart` | Root actor strategy: `restart` or `stop`. |
| `supervision.strategy` | `restart` | Child failure strategy: `restart`, `stop`, `resume`, or `escalate`. |
| `supervision.scope` | `one-for-one` | Child failure scope: `one-for-one` or `all-for-one`. |
| `supervision.max.restarts` | `3` | Maximum restarts inside the configured window. |
| `supervision.within` | `1s` | Restart counting window. Numeric values are milliseconds. |
| `supervision.backoff.min` | `10ms` | Minimum restart backoff. |
| `supervision.backoff.max` | `1s` | Maximum restart backoff. |
| `supervision.backoff.factor` | `2.0` | Exponential backoff multiplier. |
| `supervision.backoff.jitter` | `0.0` | Random backoff jitter from `0.0` to `1.0`. |

`actor.restart.strategy` controls root actor restart behavior and is intentionally independent from `supervision.strategy`. A valid supervision value such as `resume` must not be parsed as a root restart strategy.

### Remoting

| Key | Default | Meaning |
| --- | --- | --- |
| `remoting.enabled` | `false` | Start the remoting server during ActorSystem creation. |
| `remoting.host` | `127.0.0.1` | Bind host. |
| `remoting.port` | `2552` | Bind port. Port `0` requests an available OS-assigned port. |
| `remoting.stripe.count` | `8` | Number of remoting connection stripes. |

### Executor

| Key | Default | Meaning |
| --- | --- | --- |
| `executor.pool.size` | `4` | Number of executor worker fibers. |
| `executor.queue.capacity` | `128` | Maximum queued executor tasks. |

### Persistence

| Key | Default | Meaning |
| --- | --- | --- |
| `persistence.db.path` | `data/movie_persistence.sqlite3` | SQLite database file path. |
| `persistence.pool.size` | `1` | Number of serialized persistence connection actors. Values below `1` are clamped by the pool implementation. |

## Null And Errors

Explicit `null` is stored as a present configuration value. `has_path?` can distinguish it from a missing path; typed accessors reject it with `WrongTypeConfigError`. Missing values use the accessor default when one is provided, otherwise they raise `MissingConfigError`.

Malformed YAML/JSON and invalid scalar conversions are reported as `ConfigError` subclasses instead of leaking parser or conversion exceptions.

## Removed Legacy Names

This schema is pre-RC and does not retain aliases for the old inconsistent names. These paths are removed:

- `supervision.max-restarts`
- `remoting.stripe-count`
- `executor.pool-size`
- `executor.queue-capacity`
- `movie.persistence.db_path`
- `movie.persistence.pool_size`

Use the canonical dotted paths above in configuration files and environment variables.
