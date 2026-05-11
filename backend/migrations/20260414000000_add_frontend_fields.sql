-- Add reduce_only to orders
ALTER TABLE orders ADD COLUMN IF NOT EXISTS reduce_only BOOLEAN DEFAULT false;
