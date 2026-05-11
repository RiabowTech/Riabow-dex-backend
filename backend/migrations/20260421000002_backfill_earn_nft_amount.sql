-- Backfill earn_subscriptions.nft_amount for rows that were created before
-- the Subscribed log parser set nft_amount correctly. The Soulbound ERC-1155
-- NFT is always minted 1:1 against the subscribed principal, so nft_amount
-- must equal amount. Rows stored with nft_amount = 0 are the result of a
-- parser bug where the (non-existent) field was hardcoded to ZERO.
UPDATE earn_subscriptions
SET nft_amount = amount
WHERE nft_amount = 0
  AND amount > 0;
