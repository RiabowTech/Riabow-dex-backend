-- Surface friendlier metadata on the spot markets list/details endpoints.
-- `display_name` lets the FE render "Diffie / Tether" instead of "DF / USDT"
-- (the handler falls back to `BASE / QUOTE` when this column is NULL, so
-- existing markets without a curated name still get a reasonable label).
-- `description` is free-form copy shown on the market detail card; left
-- NULL by default until the team fills it in.
--
-- Both columns are nullable so the migration is a metadata-only change on
-- PG 11+ and stays safe for any markets added after this lands.

ALTER TABLE spot_markets
    ADD COLUMN IF NOT EXISTS display_name TEXT,
    ADD COLUMN IF NOT EXISTS description  TEXT;

-- Seed the only live market with a curated display name. The description
-- stays NULL on purpose — the team will fill it in (probably via the
-- admin patch endpoint we'll add in a follow-up).
UPDATE spot_markets
   SET display_name = 'Diffie / Tether'
 WHERE id = 'DFUSDT'
   AND display_name IS NULL;
