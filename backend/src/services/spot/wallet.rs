//! Spot wallet operations. The single explicit boundary point with perp:
//! `transfer` reads/writes the `balances` table directly via SQL.

use rust_decimal::Decimal;
use sqlx::PgPool;

use crate::models::spot::{SpotBalance, TransferDirection};

#[derive(Debug)]
pub struct TransferResult {
    pub perp_balance_after: Decimal,
    pub spot_balance_after: Decimal,
}

#[derive(thiserror::Error, Debug)]
pub enum WalletError {
    #[error("insufficient balance")]
    InsufficientBalance,
    #[error("unsupported token: {0}")]
    UnsupportedToken(String),
    #[error("amount must be positive")]
    NonPositiveAmount,
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

/// Move USDT atomically between perp `balances` and `spot_balances`.
/// MVP guard: token must be "USDT".
pub async fn transfer(
    pool: &PgPool,
    user_address: &str,
    direction: TransferDirection,
    token: &str,
    amount: Decimal,
) -> Result<TransferResult, WalletError> {
    if token != "USDT" {
        return Err(WalletError::UnsupportedToken(token.to_string()));
    }
    if amount <= Decimal::ZERO {
        return Err(WalletError::NonPositiveAmount);
    }

    let user = user_address.to_lowercase();
    let mut tx = pool.begin().await?;

    let perp_before: Decimal = sqlx::query_scalar(
        "SELECT available FROM balances
          WHERE user_address = $1 AND token = 'USDT' FOR UPDATE"
    )
    .bind(&user)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(Decimal::ZERO);

    let spot_before: Decimal = sqlx::query_scalar(
        "SELECT available FROM spot_balances
          WHERE user_address = $1 AND token = 'USDT' FOR UPDATE"
    )
    .bind(&user)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(Decimal::ZERO);

    let (perp_after, spot_after) = match direction {
        TransferDirection::PerpToSpot => {
            if perp_before < amount {
                return Err(WalletError::InsufficientBalance);
            }
            sqlx::query(
                "UPDATE balances SET available = available - $1, updated_at = NOW()
                  WHERE user_address = $2 AND token = 'USDT'"
            )
            .bind(amount).bind(&user)
            .execute(&mut *tx).await?;

            sqlx::query(
                "INSERT INTO spot_balances (user_address, token, available, frozen)
                 VALUES ($1, 'USDT', $2, 0)
                 ON CONFLICT (user_address, token)
                 DO UPDATE SET available = spot_balances.available + EXCLUDED.available,
                               updated_at = NOW()"
            )
            .bind(&user).bind(amount)
            .execute(&mut *tx).await?;

            (perp_before - amount, spot_before + amount)
        }
        TransferDirection::SpotToPerp => {
            if spot_before < amount {
                return Err(WalletError::InsufficientBalance);
            }
            sqlx::query(
                "UPDATE spot_balances SET available = available - $1, updated_at = NOW()
                  WHERE user_address = $2 AND token = 'USDT'"
            )
            .bind(amount).bind(&user)
            .execute(&mut *tx).await?;

            sqlx::query(
                "INSERT INTO balances (user_address, token, available, frozen)
                 VALUES ($1, 'USDT', $2, 0)
                 ON CONFLICT (user_address, token)
                 DO UPDATE SET available = balances.available + EXCLUDED.available,
                               updated_at = NOW()"
            )
            .bind(&user).bind(amount)
            .execute(&mut *tx).await?;

            (perp_before + amount, spot_before - amount)
        }
    };

    sqlx::query(
        "INSERT INTO spot_internal_transfers
           (user_address, direction, token, amount,
            perp_balance_before, spot_balance_before)
         VALUES ($1, $2, 'USDT', $3, $4, $5)"
    )
    .bind(&user).bind(direction.as_str()).bind(amount)
    .bind(perp_before).bind(spot_before)
    .execute(&mut *tx).await?;

    tx.commit().await?;

    Ok(TransferResult {
        perp_balance_after: perp_after,
        spot_balance_after: spot_after,
    })
}

/// Read all spot balances for a user.
pub async fn list_balances(pool: &PgPool, user_address: &str) -> Result<Vec<SpotBalance>, WalletError> {
    let user = user_address.to_lowercase();
    let rows = sqlx::query_as::<_, SpotBalance>(
        "SELECT * FROM spot_balances WHERE user_address = $1 ORDER BY token"
    )
    .bind(&user)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use rust_decimal_macros::dec;

    async fn setup_pool() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must be set for integration tests");
        let pool = PgPoolOptions::new().max_connections(5)
            .connect(&url).await.expect("connect");
        // Apply spot schema (idempotent).
        let sql = include_str!("../../../../scripts/spot_subsystem_bootstrap.sql");
        sqlx::raw_sql(sql).execute(&pool).await.expect("bootstrap");
        pool
    }

    async fn seed_perp_balance(pool: &PgPool, user: &str, available: Decimal) {
        sqlx::query(
            "INSERT INTO balances (user_address, token, available, frozen)
             VALUES ($1, 'USDT', $2, 0)
             ON CONFLICT (user_address, token)
             DO UPDATE SET available = EXCLUDED.available, frozen = 0"
        ).bind(user.to_lowercase()).bind(available)
         .execute(pool).await.unwrap();
    }

    async fn cleanup(pool: &PgPool, user: &str) {
        let u = user.to_lowercase();
        sqlx::query("DELETE FROM spot_internal_transfers WHERE user_address=$1").bind(&u).execute(pool).await.unwrap();
        sqlx::query("DELETE FROM spot_balances WHERE user_address=$1").bind(&u).execute(pool).await.unwrap();
        sqlx::query("DELETE FROM balances WHERE user_address=$1").bind(&u).execute(pool).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "needs TEST_DATABASE_URL"]
    async fn perp_to_spot_first_time_creates_spot_row() {
        let pool = setup_pool().await;
        let user = "0x1111111111111111111111111111111111111111";
        cleanup(&pool, user).await;
        seed_perp_balance(&pool, user, dec!(100)).await;

        let result = transfer(&pool, user, TransferDirection::PerpToSpot, "USDT", dec!(40))
            .await.unwrap();

        assert_eq!(result.perp_balance_after, dec!(60));
        assert_eq!(result.spot_balance_after, dec!(40));
        cleanup(&pool, user).await;
    }

    #[tokio::test]
    #[ignore = "needs TEST_DATABASE_URL"]
    async fn perp_to_spot_insufficient_returns_error() {
        let pool = setup_pool().await;
        let user = "0x2222222222222222222222222222222222222222";
        cleanup(&pool, user).await;
        seed_perp_balance(&pool, user, dec!(10)).await;

        let result = transfer(&pool, user, TransferDirection::PerpToSpot, "USDT", dec!(100)).await;
        assert!(matches!(result, Err(WalletError::InsufficientBalance)));
        cleanup(&pool, user).await;
    }

    #[tokio::test]
    #[ignore = "needs TEST_DATABASE_URL"]
    async fn round_trip_preserves_total() {
        let pool = setup_pool().await;
        let user = "0x3333333333333333333333333333333333333333";
        cleanup(&pool, user).await;
        seed_perp_balance(&pool, user, dec!(100)).await;

        transfer(&pool, user, TransferDirection::PerpToSpot, "USDT", dec!(70)).await.unwrap();
        transfer(&pool, user, TransferDirection::SpotToPerp, "USDT", dec!(30)).await.unwrap();
        transfer(&pool, user, TransferDirection::PerpToSpot, "USDT", dec!(20)).await.unwrap();

        let perp: Decimal = sqlx::query_scalar(
            "SELECT available FROM balances WHERE user_address=$1 AND token='USDT'"
        ).bind(user.to_lowercase()).fetch_one(&pool).await.unwrap();
        let spot: Decimal = sqlx::query_scalar(
            "SELECT available FROM spot_balances WHERE user_address=$1 AND token='USDT'"
        ).bind(user.to_lowercase()).fetch_one(&pool).await.unwrap();

        assert_eq!(perp + spot, dec!(100));
        cleanup(&pool, user).await;
    }

    #[tokio::test]
    #[ignore = "needs TEST_DATABASE_URL"]
    async fn unsupported_token_rejected() {
        let pool = setup_pool().await;
        let user = "0x4444444444444444444444444444444444444444";
        cleanup(&pool, user).await;
        let result = transfer(&pool, user, TransferDirection::PerpToSpot, "DF", dec!(1)).await;
        assert!(matches!(result, Err(WalletError::UnsupportedToken(_))));
        cleanup(&pool, user).await;
    }

    #[tokio::test]
    #[ignore = "needs TEST_DATABASE_URL"]
    async fn negative_amount_rejected() {
        let pool = setup_pool().await;
        let user = "0x5555555555555555555555555555555555555555";
        cleanup(&pool, user).await;
        let result = transfer(&pool, user, TransferDirection::PerpToSpot, "USDT", dec!(-1)).await;
        assert!(matches!(result, Err(WalletError::NonPositiveAmount)));
        cleanup(&pool, user).await;
    }
}
