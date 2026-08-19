# Epic 05: Quality Gates and Documentation Accuracy

**Goal:** Make the repository safer to change by default and easier to understand for the next implementation wave.

**Why this epic exists:** The project currently relies on manual discipline, the README is too thin for the actual feature surface, and correctness checks are mixed with benchmark-style specs that are noisy in normal runs.

**Depends on:** Epic 01 through Epic 04 ideally complete, but some tasks can start earlier.

**Done when:**

- the repository has explicit correctness gates,
- documentation reflects feature maturity honestly,
- default verification is quieter and more intentional,
- epic and workflow docs are easy to discover.

## Task 05.1: Separate correctness specs from benchmark and stress runs

**Files**

- Modify: `spec/movie/remote/benchmark_spec.cr`
- Modify: `spec/movie/remote/stress_spec.cr`
- Modify: `README.md`

**Outcome**

- Keep correctness specs in the default suite.
- Move benchmarks and stress-style checks behind an explicit flag or separate command path.

**Verification**

- Run the default spec command and confirm the default output stays focused on correctness.
- Run the opt-in benchmark/stress command separately.

## Task 05.2: Add CI for spec and example build verification

**Files**

- Add: `.github/workflows/ci.yml`
- Modify: `README.md`

**Outcome**

- Run the default spec command in CI.
- Build examples in CI so public snippets do not silently rot.

**Verification**

- Validate the workflow locally as far as practical.
- Confirm YAML and referenced commands are correct.

## Task 05.3: Expand README into a real project entry point

**Files**

- Modify: `README.md`

**Outcome**

- Document feature maturity, installation, core usage, persistence, remoting status, streams, and verification commands.
- Link to deeper docs in `doc/movie`.

**Verification**

- Build any examples referenced by new README snippets.

## Task 05.4: Add a docs index and cross-links

**Files**

- Add: `doc/movie/README.md`
- Modify: `doc/movie/actor_lifecycle.md`
- Modify: `doc/movie/streams.md`

**Outcome**

- Make `doc/movie` browsable from a single page.
- Link the backlog, workflow, architecture docs, and epics together.

**Verification**

- Read through the docs from the new index and confirm every link resolves.

## Task 05.5: Enforce workflow rules in contributor-facing docs

**Files**

- Modify: `README.md`
- Modify: `doc/movie/development_workflow.md`

**Outcome**

- Make TDD, verification, and mandatory review part of normal contributor guidance rather than tribal knowledge.

**Verification**

- Confirm the workflow doc and README do not contradict each other.

