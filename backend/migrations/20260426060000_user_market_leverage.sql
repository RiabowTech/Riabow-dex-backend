-- Per-user, per-symbol leverage selection used by /fapi/v1/leverage.
-- Binance fapi clients can't pass leverage on each /fapi/v1/order; they
-- set it once via /fapi/v1/leverage and expect it to apply on every
-- subsequent order for that symbol. Before this table the endpoint just
-- echoed the value back without persisting, so /fapi/v1/order always
-- used the market's max_leverage — silent over-leverage for any MM
-- relying on the documented Binance behavior.
--
-- The JWT path (/api/v1/orders) keeps per-order leverage as-is; this
-- table is only consulted when the fapi handler needs a default.

CREATE TABLE IF NOT EXISTS user_market_leverage (
    user_address VARCHAR(42) NOT NULL,
    symbol       VARCHAR(20) NOT NULL,
    leverage     INTEGER     NOT NULL CHECK (leverage > 0),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_address, symbol)
);
