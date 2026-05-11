//! One-shot baseline tool that marks every embedded migration as
//! already-applied in `_sqlx_migrations`. Use against environments
//! whose schema was hand-built before sqlx migrations were adopted
//! (sepolia, mainnet as of 2026-05-09).
//!
//! Idempotent: ON CONFLICT (version) DO NOTHING. Safe to re-run.
//!
//! Usage:
//!     DATABASE_URL=postgres://... cargo run --bin sqlx_baseline
//!     DATABASE_URL=postgres://... cargo run --bin sqlx_baseline -- --dry-run
//!
//! Run once per environment, then never again.
//!
//! WARNING: do NOT run against a fresh DB whose schema has not been
//! applied yet. This tool only marks migrations as applied; it does
//! not apply them. For fresh DBs, use `sqlx migrate run` or wait for
//! the runtime auto-migrate (PR 2).

use sqlx::migrate::Migrator;
use sqlx::PgPool;

static MIGRATOR: Migrator = sqlx::migrate!();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dry_run = std::env::args().any(|a| a == "--dry-run");

    let url = std::env::var("DATABASE_URL")
        .map_err(|_| anyhow::anyhow!("DATABASE_URL not set"))?;

    let pool = PgPool::connect(&url).await?;

    // Match the schema sqlx uses internally so that a future
    // sqlx::migrate!().run() finds our seeded rows. Schema is from
    // sqlx-postgres 0.7's migrate module.
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS _sqlx_migrations (
            version        BIGINT PRIMARY KEY,
            description    TEXT NOT NULL,
            installed_on   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            success        BOOLEAN NOT NULL,
            checksum       BYTEA NOT NULL,
            execution_time BIGINT NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await?;

    let mut seeded = 0usize;
    let mut already = 0usize;
    let total = MIGRATOR.iter().count();

    for m in MIGRATOR.iter() {
        if dry_run {
            println!(
                "[dry-run] would insert: version={} description='{}' checksum={} bytes",
                m.version,
                m.description,
                m.checksum.len()
            );
            continue;
        }

        let res = sqlx::query(
            r#"
            INSERT INTO _sqlx_migrations
                (version, description, installed_on, success, checksum, execution_time)
            VALUES ($1, $2, NOW(), TRUE, $3, 0)
            ON CONFLICT (version) DO NOTHING
            "#,
        )
        .bind(m.version)
        .bind(&*m.description)
        .bind(&m.checksum[..])
        .execute(&pool)
        .await?;

        if res.rows_affected() == 1 {
            seeded += 1;
            println!("seeded {} '{}'", m.version, m.description);
        } else {
            already += 1;
            println!("already present {} '{}'", m.version, m.description);
        }
    }

    if dry_run {
        println!("[dry-run] would have processed {} migrations.", total);
    } else {
        println!(
            "done. seeded {} new, {} already present (out of {} embedded).",
            seeded, already, total
        );
    }
    Ok(())
}
