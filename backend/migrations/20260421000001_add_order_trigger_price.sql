-- Add trigger_price column to orders table to support TP/SL order types
ALTER TABLE orders ADD COLUMN IF NOT EXISTS trigger_price NUMERIC;
