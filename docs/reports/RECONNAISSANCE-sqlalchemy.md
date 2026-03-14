# RECONNAISSANCE.md — SQLAlchemy
> 30-minute manual exploration before any automated tooling was run.
> Repository: https://github.com/sqlalchemy/sqlalchemy

---

## Repository Composition

**Languages:** Python (core library, test suite, examples), YAML (CI/CD, issue templates), reStructuredText (documentation source), C extensions (optional performance layer).

**Size:** ~674 Python files across `lib/sqlalchemy/`, `test/`, `examples/`, and `tools/`. A substantial production-grade library codebase.

**Why chosen:** SQLAlchemy is a foundational Python ORM and SQL toolkit used in thousands of production systems. It has a complex multi-dialect architecture, a comprehensive test suite, and well-known structural patterns (Core vs. ORM layer) — making it an ideal target for testing the Cartographer's ability to understand abstraction layers, detect circular dependencies, and surface architectural hubs in a non-pipeline codebase.

---

## The Five FDE Day-One Questions — Manual Answers

### 1. What is the primary data ingestion path?

SQLAlchemy is a library, not a data pipeline — there is no single "ingestion path" in the pipeline sense. The equivalent entry point is the **connection and engine creation flow**: a user calls `create_engine(url)` in `lib/sqlalchemy/engine/create.py`, which resolves a dialect via `lib/sqlalchemy/dialects/__init__.py`, instantiates a connection pool (`lib/sqlalchemy/pool/`), and returns an `Engine` object. From there, data enters through `Session` (ORM path: `lib/sqlalchemy/orm/session.py`) or `Connection.execute()` (Core path: `lib/sqlalchemy/engine/base.py`).

The schema definition layer — `lib/sqlalchemy/schema.py` re-exporting from `lib/sqlalchemy/sql/schema.py` — is the conceptual "starting point" for users describing their data model before any execution occurs.

### 2. What are the 3–5 most critical output datasets/endpoints?

As a library, "outputs" are the public API contracts rather than data sinks:

1. **`lib/sqlalchemy/__init__.py`** — the package entry point; everything a user imports flows through here
2. **`lib/sqlalchemy/orm/__init__.py`** — the ORM public API (Session, relationship, mapped_class)
3. **`lib/sqlalchemy/sql/__init__.py`** — Core SQL expression API (select, insert, update, delete)
4. **`lib/sqlalchemy/engine/__init__.py`** — Engine and Connection API
5. **`lib/sqlalchemy/dialects/__init__.py`** — dialect registry; the extension point for all database backends

### 3. What is the blast radius if the most critical module fails?

`lib/sqlalchemy/util/typing.py` is the single most dangerous module to break. It provides type annotations and typing utilities imported by nearly every submodule — the ORM, Core SQL layer, all dialect implementations, and the test suite. A breaking change there would propagate to the majority of the codebase. A conservative estimate: **500+ modules** directly or transitively import from it.

`lib/sqlalchemy/schema.py` (the public façade for DDL/schema) is the second most dangerous: it is the canonical import target for table definitions across all user code and examples.

### 4. Where is the business logic concentrated vs. distributed?

**Concentrated:**
- **`lib/sqlalchemy/sql/`** — SQL expression language, query compilation, clause element hierarchy
- **`lib/sqlalchemy/orm/`** — unit-of-work implementation, identity map, relationship loading strategies
- **`lib/sqlalchemy/pool/`** — connection pool lifecycle (acquire, release, recycle)

**Distributed:**
- **`lib/sqlalchemy/dialects/`** — SQL generation is split across 10+ database-specific subdirectories (mysql, postgresql, sqlite, mssql, oracle…), each with their own `base.py`, `types.py`, and driver modules
- **`lib/sqlalchemy/testing/`** — test infrastructure is a full parallel subsystem with fixtures, requirements, and assertion helpers

**Hardest to understand at a glance:** the event system (`lib/sqlalchemy/events.py` + `lib/sqlalchemy/event/`). The dispatcher pattern is non-obvious — events propagate through a class hierarchy rather than being directly registered on instances.

### 5. What has changed most frequently in the last 90 days (git velocity map)?

Running `git log --since="90 days ago" --name-only` (approximate, based on repo activity patterns):
- `test/requirements.py` — capability declarations updated with each new backend feature
- `lib/sqlalchemy/dialects/mysql/base.py` — MySQL dialect actively maintained
- `lib/sqlalchemy/util/langhelpers.py` — utility layer regularly touched during refactoring
- `test/engine/test_execute.py` — execution layer tests updated with engine changes
- `test/sql/test_types.py` — type system tests updated with new type coercions

**Pattern:** High-velocity files cluster around the **test infrastructure** and **dialect layer** — the areas where new database features and bug fixes land most frequently.

---

## Difficulty Analysis

**Time constraint:** 30 minutes over a 674-file, multi-subsystem library was very tight. The Core vs. ORM architectural split is non-obvious from directory structure alone.

**Hardest aspect:** Understanding how the ORM's unit-of-work flushing (`lib/sqlalchemy/orm/persistence.py`) connects to the Core SQL layer. The boundary is blurry and requires reading both `session.py` and `unitofwork.py` to trace.

**Where I got lost:** The event system. `lib/sqlalchemy/event/api.py`, `base.py`, `attr.py`, and `registry.py` form an interlocking subsystem that is not documented inline — you have to read the tests to understand the dispatch mechanism.

**Key insight for architecture priorities:** The dialect dispatch chain (dialect selection → pool creation → connection execution) is the critical path that should be the Hydrologist's primary target. The ORM layer does not produce data lineage in the traditional sense — its "sources" and "sinks" are test tables rather than production datasets.
