-- Add four new order types to the order_type enum:
-- take_profit_limit, stop_loss_limit, take_profit_market, stop_loss_market
--
-- These represent triggered orders:
-- - take_profit_limit / stop_loss_limit: once triggered, behave as limit orders
-- - take_profit_market / stop_loss_market: once triggered, behave as market orders

ALTER TYPE order_type ADD VALUE IF NOT EXISTS 'take_profit_limit';
ALTER TYPE order_type ADD VALUE IF NOT EXISTS 'stop_loss_limit';
ALTER TYPE order_type ADD VALUE IF NOT EXISTS 'take_profit_market';
ALTER TYPE order_type ADD VALUE IF NOT EXISTS 'stop_loss_market';
