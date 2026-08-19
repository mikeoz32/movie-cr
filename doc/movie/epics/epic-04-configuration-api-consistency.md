# Epic 04: Configuration API Consistency

**Status:** Completed
**Goal:** Make configuration behavior predictable, documented, and internally consistent across all feature areas.

**Why this epic exists:** The config API currently conflates `nil` with missing paths, leaks raw parsing exceptions, mixes naming conventions, and overloads supervision settings in a way that breaks valid configurations.

**Depends on:** None.

**Done when:**

- config path semantics are unambiguous,
- type errors are reported as config errors,
- restart strategy config is separate from supervision strategy,
- naming and env override conventions are consistent and documented.

## Task 04.1: Decide null semantics and encode them in tests

**Files**

- Modify: `src/movie/config.cr`
- Modify: `spec/movie/config_spec.cr`

**Outcome**

- [x] Either support explicit `null` as a first-class stored value or remove `Nil` from the supported public model.
- [x] Align `has_path?`, `[]`, `[]?`, and `get_value!` with that decision.

**Verification**

- Add failing tests for `null` handling.
- Run `crystal spec spec/movie/config_spec.cr -Dpreview_mt -Dexecution_context`.

## Task 04.2: Normalize config error handling

**Files**

- Modify: `src/movie/config.cr`
- Modify: `spec/movie/config_spec.cr`

**Outcome**

- [x] Wrap raw parsing failures so callers see `ConfigError` or `WrongTypeConfigError` consistently.
- [x] Cover integer, float, bool, and duration parsing failures.

**Verification**

- Add failing parser-error tests first.
- Run targeted config specs and full suite.

## Task 04.3: Split actor restart strategy from supervision strategy

**Files**

- Modify: `src/movie/config.cr`
- Modify: `src/movie.cr`
- Modify: `spec/movie/config_spec.cr`
- Modify: `spec/movie/actor_system_spec.cr`

**Outcome**

- [x] Introduce a separate config key for root actor restart behavior.
- [x] Keep supervision strategy values independent from actor restart values.

**Verification**

- Add a failing spec for `supervision.strategy = resume` not breaking system creation.
- Run targeted config and actor-system specs.

## Task 04.4: Canonicalize key naming and env overrides

**Files**

- Modify: `src/movie/config.cr`
- Modify: `src/movie/persistence.cr`
- Modify: `src/movie/executor.cr`
- Modify: `spec/movie/config_spec.cr`

**Outcome**

- [x] Choose one canonical naming scheme for nested keys and hyphenated segments.
- [x] Make env overrides map cleanly to canonical keys.
- [x] Keep legacy persistence keys and single-underscore environment variables as compatibility aliases.

**Verification**

- Add failing env-override tests first.
- Run targeted config specs and full suite.

## Task 04.5: Publish the supported config schema

**Files**

- Modify: `README.md`
- Add: `doc/movie/configuration.md`

**Outcome**

- [x] Document supported keys, defaults, aliases, and feature-specific config sections.
- [x] Include persistence, executor, and remoting settings.

**Verification**

- Build any touched examples if their config usage changes.

## Review Notes

- The final full MT suite and all example builds pass on the completed Epic 04 branch.
- Remote Watch followed immediately by Stop has a pre-existing asynchronous ordering flake in the remoting E2E suite; it is outside this configuration scope and remains a runtime follow-up.
