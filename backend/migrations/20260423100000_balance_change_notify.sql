-- 2026-04-23: event-driven balance push via Postgres LISTEN/NOTIFY
--
-- Motivation: the WS `balances` channel currently pushes via a 5-second
-- `private_interval.tick()` poll on the DB. Bots can't rely on this for
-- their freeze/available accounting, so they fall back to REST polling
-- at >10 req/s per bot (1.14M calls/day globally, ~25% of one CPU core).
--
-- Going event-driven at the WS layer needs a signal whenever `balances`
-- changes. Instrumenting every UPDATE site individually (order.rs freeze,
-- orchestrator maker-release, position/mod.rs opposing close, withdraw,
-- deposit credit, etc. — a dozen places) is a refactoring hazard. A
-- single AFTER INSERT OR UPDATE trigger on `balances` that calls
-- pg_notify covers all writers unconditionally.
--
-- The backend's new balance listener task (see bootstrap.rs) calls
-- LISTEN balance_change on a dedicated long-lived connection, decodes
-- the payload, and fans out into `balance_update_sender`. The WS
-- handler subscribes to that channel and pushes matching events to
-- the authenticated user.
--
-- Payload: JSON with `user_address`, `token`, `available`, `frozen`.
-- Decimals are serialized as text so the receiving Rust side can
-- parse them with rust_decimal without going through f64.

CREATE OR REPLACE FUNCTION notify_balance_change()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify(
    'balance_change',
    json_build_object(
      'user_address', NEW.user_address,
      'token', NEW.token,
      'available', NEW.available::text,
      'frozen', NEW.frozen::text
    )::text
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_balance_change ON balances;
CREATE TRIGGER trg_balance_change
AFTER INSERT OR UPDATE ON balances
FOR EACH ROW
EXECUTE FUNCTION notify_balance_change();
