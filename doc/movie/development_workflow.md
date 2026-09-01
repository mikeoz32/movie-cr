# Movie Development Workflow

This document defines the minimum process for implementing any task in this repository.

## Non-Negotiable Rules

### 1. Work from an explicit task

No implementation starts without a named epic task. If work is discovered mid-task, either:

- fold it into the current task if it is required for correctness, or
- create a follow-up task before touching unrelated code.

### 2. TDD is mandatory

No production code may be written before a failing test exists.

Required sequence:

1. Write one failing test for one behavior.
2. Run the targeted command and confirm it fails for the expected reason.
3. Write the minimum production change to make it pass.
4. Re-run the targeted command and confirm it passes.
5. Refactor only while keeping tests green.

Allowed exceptions must be explicit and rare:

- generated code,
- pure documentation files,
- repository metadata with no executable behavior.

### 3. Verification is mandatory

No task may be marked done without fresh command output from the current branch.

Minimum verification stack:

1. Formatting check for Crystal sources.
2. Targeted spec command for the changed behavior.
3. Relevant broader suite for the touched subsystem.
4. Example build or runtime smoke check if public API or examples changed.

Baseline commands for this repository:

```bash
crystal tool format --check src spec examples
crystal spec spec/movie -Dpreview_mt -Dexecution_context
```

Benchmark and stress specs are opt-in and are not part of the default correctness suite:

```bash
MOVIE_BENCH=1 crystal spec --release spec/movie/remote/benchmark_spec.cr -Dpreview_mt -Dexecution_context
MOVIE_BENCH=1 crystal spec --release spec/movie/remote/association_benchmark_spec.cr -Dpreview_mt -Dexecution_context
MOVIE_STRESS=1 crystal spec spec/movie/remote/stress_spec.cr -Dpreview_mt -Dexecution_context
MOVIE_BENCH=1 crystal spec spec/movie/persistence_benchmark_spec.cr -Dpreview_mt -Dexecution_context
crystal build benchmarks/actor_system.cr --release -Dpreview_mt -Dexecution_context -o /tmp/movie-actor-system-benchmark
```

Benchmark runs report measurements without host-dependent pass/fail thresholds. Compare results only across repeated runs on the same environment. ActorSystem throughput measurements must use receiver-side completion barriers; socket enqueue completion is not an end-to-end delivery measurement.

When examples are touched, also run:

```bash
for f in examples/*.cr; do crystal build "$f" -Dpreview_mt -Dexecution_context -o /tmp/$(basename "$f" .cr); done
```

### 4. Review after every completed task is mandatory

A task is not complete after tests pass. It is complete only after a review pass is requested and handled.

Required sequence after verification:

1. Summarize what changed.
2. Request review against the task scope.
3. Fix critical and important findings before moving on.
4. Record any intentionally deferred minor items in the task notes.

If using Codex workflows, use the local `code-review` skill against the task's fixed starting point.

### 5. Keep changes small

- Prefer one task per commit, or a very tight pair of related tasks.
- Do not mix refactors with behavior changes unless the task explicitly requires it.
- Do not quietly expand scope because a nearby cleanup looks tempting.

### 6. Update docs when behavior changes

If a task changes public API, operational behavior, feature maturity, or example code, update the relevant docs in the same task.

## Task Completion Checklist

Copy this checklist into task notes when executing an epic:

- [ ] Failing test written first
- [ ] Failing test observed red
- [ ] Minimal implementation written
- [ ] Targeted verification green
- [ ] Broader verification green
- [ ] Formatting check green
- [ ] Docs/examples updated if needed
- [ ] Review requested
- [ ] Review feedback addressed

## Repository-Specific Guidance

- Treat remoting as an experimental MVP and follow [remoting.md](remoting.md) for its supported protocol and limitations.
- Treat persistence as stable enough to extend, but require regression tests before touching recovery or storage semantics.
- Keep stdout noise out of default runtime paths; use structured logging instead.
