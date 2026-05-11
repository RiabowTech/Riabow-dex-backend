# Contributing to Riabow Exchange

First off — **thank you** for considering contributing to Riabow. Open, decentralized finance is built by communities, and every bug report, doc fix, test case, and feature PR moves the protocol forward.

This document is a short guide. If anything is unclear, please open a Discussion or ask in the [community Discord](https://discord.gg/riabow) — we're happy to help.

---

## Code of Conduct

This project and everyone participating in it is governed by the [Code of Conduct](./CODE_OF_CONDUCT.md). By participating, you are expected to uphold it. Please report unacceptable behavior to `conduct@riabow.exchange`.

## How Can I Contribute?

### 🐛 Reporting Bugs

Before opening a bug report, please:

1. **Search existing issues** to make sure it hasn't been reported already.
2. **Reproduce on the latest `main`** if possible.
3. **Open an issue** using the bug template. Include:
   - A clear, descriptive title
   - Exact steps to reproduce
   - Expected vs. actual behavior
   - Environment (OS, Rust version, Postgres version, network)
   - Relevant logs, stack traces, and Prometheus metrics if available

> 🔒 **Security issues should NOT be filed as public issues.** See [SECURITY.md](./SECURITY.md) for our private disclosure process.

### 💡 Suggesting Enhancements

Open a GitHub Discussion under **Ideas** before writing significant code. This lets the community weigh in on design and saves you from building something that ends up not landing. For small enhancements, a regular issue is fine.

A good enhancement proposal answers:

- What problem does this solve?
- Who benefits, and how?
- What are the trade-offs?
- Are there alternatives you considered?

### 🔧 Pull Requests

We welcome PRs of every size — from typo fixes to entire new market modules.

#### Workflow

1. **Fork** the repository and clone your fork.
2. **Create a topic branch** off `main`:
   ```bash
   git checkout -b feat/your-feature
   # or
   git checkout -b fix/issue-1234
   ```
3. **Make focused commits.** One logical change per commit; one logical feature per PR.
4. **Run the local checks** (see [Development Setup](#development-setup)).
5. **Push and open a Pull Request** against `main`.
6. **Respond to review feedback.** We aim to do a first pass within two business days.

#### Pull Request Checklist

- [ ] PR title follows [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `perf:`)
- [ ] Description explains **what** changed and **why**
- [ ] `cargo fmt --check` passes
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo test` passes locally
- [ ] New code is covered by tests where reasonable
- [ ] Database changes ship with a new `sqlx` migration in `backend/migrations/`
- [ ] Public API changes are documented
- [ ] No secrets, private keys, or credentials in commits

#### PR Size

We strongly prefer **small, focused PRs**. If your change touches more than ~500 lines of non-generated code, consider splitting it. Large PRs are harder to review, more likely to introduce regressions, and slower to merge.

## Development Setup

### Prerequisites

- Rust ≥ 1.75 (stable)
- PostgreSQL ≥ 15
- Redis ≥ 7
- `sqlx-cli` (`cargo install sqlx-cli --no-default-features --features postgres`)

### Getting Started

```bash
# Clone and enter
git clone https://github.com/<your-username>/riabow-dex-backend.git
cd riabow-dex-backend/backend

# Configure
cp .env.example .env
# Edit .env with your local Postgres/Redis credentials

# Migrate
sqlx migrate run

# Run
cargo run
```

### Local Checks (run before pushing)

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

A pre-push hook is recommended:

```bash
# .git/hooks/pre-push
#!/usr/bin/env bash
set -e
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

## Coding Guidelines

### Rust Style

- Follow standard `rustfmt` and `clippy` lints. We treat clippy warnings as errors in CI.
- Prefer `Result<T, E>` with `thiserror` over `anyhow` for library code; `anyhow` is fine in binaries and tests.
- Avoid `unwrap()` and `expect()` outside of tests and one-shot startup code.
- Use `tracing` for logs, not `println!` / `eprintln!`.
- Keep modules focused — if a file passes ~600 lines, consider splitting.

### Async & Concurrency

- All public service methods should be `async` and `Send + Sync`.
- Use `parking_lot::Mutex` for short critical sections; `tokio::sync::Mutex` only when holding across `.await` is unavoidable.
- Be mindful of `Arc` clones — they're cheap but allocations add up on hot paths.
- Never block the runtime. Wrap CPU-bound work in `tokio::task::spawn_blocking`.

### Database

- Every schema change ships as a new `sqlx` migration. **Never edit a committed migration.**
- Use parameterized queries — `sqlx::query!` / `sqlx::query_as!`. No string interpolation.
- Add indexes for any column you query on in production code paths.
- Backfills go in their own migration, separate from schema changes.

### Testing

- Unit tests live next to the code they test (`#[cfg(test)] mod tests`).
- Integration tests live in `backend/tests/` and may require Postgres + Redis.
- Tests must be deterministic. No real network calls, no real wallets, no real time. Use the trait abstractions for external clients and inject mocks at the edges.
- New business logic without a test is a strong signal to ask for one in review.

### Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(matching): add iceberg order support
fix(funding-rate): correct premium index on negative basis
docs(readme): clarify EIP-712 signing flow
refactor(spot): extract reconciler into its own module
```

The first line stays under 72 characters. The body, if any, explains *why* — the code already shows *what*.

## Architecture Decisions

For non-trivial design changes, please write a short Architecture Decision Record (ADR) in `docs/adr/`. An ADR is one page or less and captures:

- **Context** — what's happening that requires a decision
- **Decision** — what we're going to do
- **Consequences** — what we accept as trade-offs

This makes it dramatically easier for future contributors to understand *why* the code looks the way it does.

## Releasing

Releases are cut by maintainers. The general flow:

1. All changes are merged into `main` behind feature flags when needed.
2. Maintainers tag `vX.Y.Z` following [SemVer](https://semver.org/).
3. CI builds release artifacts and publishes the changelog.

## Community

- 💬 **Discord** — [discord.gg/riabow](https://discord.gg/riabow)
- 🐦 **Twitter / X** — [@RiabowExchange](https://twitter.com/RiabowExchange)
- 🧠 **GitHub Discussions** — design discussions, ideas, Q&A
- 📨 **Email** — `hello@riabow.exchange`

---

Thanks again. We're glad you're here. 🦀
