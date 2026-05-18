# Movie

A lightweight actor framework for Crystal with typed actors, supervision, remoting, persistence, futures, ask-pattern support, and typed streams.

## Features

- Typed actor system and actor references
- Supervision and lifecycle hooks
- Ask pattern and futures
- Persistence helpers and SQLite-backed stores
- Remoting support
- Typed streams

## Installation

Add this to your `shard.yml`:

```yaml
dependencies:
  movie:
    github: mikeoz32/movie
```

## Usage

```crystal
require "movie"
```

## Development

Run specs:

```bash
crystal spec spec/movie -Dpreview_mt -Dexecution_context
```
