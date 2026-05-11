-- Fix referral_codes.tier default and recalculate existing tiers
-- New tier system (AND criteria: referral count + referred trading volume):
--   0 = Starter  10%  ≥1  person  + ≥$1,000
--   1 = Bronze   12%  ≥5  people  + ≥$10,000
--   2 = Silver   17%  ≥20 people  + ≥$100,000
--   3 = Gold     22%  ≥50 people  + ≥$500,000
--   4 = Diamond  25%  ≥100 people + ≥$2,000,000

-- 1. Fix default value
ALTER TABLE referral_codes ALTER COLUMN tier SET DEFAULT 0;

-- 2. Recalculate all existing tiers with new rules
WITH volumes AS (
    SELECT referrer_address, COALESCE(SUM(volume), 0) AS total_vol
    FROM referral_earnings
    GROUP BY referrer_address
)
UPDATE referral_codes rc
SET
    tier = CASE
        WHEN rc.total_referrals >= 100 AND COALESCE(v.total_vol, 0) >= 2000000 THEN 4
        WHEN rc.total_referrals >= 50  AND COALESCE(v.total_vol, 0) >= 500000  THEN 3
        WHEN rc.total_referrals >= 20  AND COALESCE(v.total_vol, 0) >= 100000  THEN 2
        WHEN rc.total_referrals >= 5   AND COALESCE(v.total_vol, 0) >= 10000   THEN 1
        ELSE 0
    END,
    commission_rate = CASE
        WHEN rc.total_referrals >= 100 AND COALESCE(v.total_vol, 0) >= 2000000 THEN 0.25
        WHEN rc.total_referrals >= 50  AND COALESCE(v.total_vol, 0) >= 500000  THEN 0.22
        WHEN rc.total_referrals >= 20  AND COALESCE(v.total_vol, 0) >= 100000  THEN 0.17
        WHEN rc.total_referrals >= 5   AND COALESCE(v.total_vol, 0) >= 10000   THEN 0.12
        ELSE 0.10
    END
FROM volumes v
WHERE v.referrer_address = rc.owner_address;

-- Also reset rows that had no earnings at all (no volumes entry)
UPDATE referral_codes rc
SET tier = 0, commission_rate = 0.10
WHERE NOT EXISTS (
    SELECT 1 FROM referral_earnings re WHERE re.referrer_address = rc.owner_address
)
AND rc.tier != 0;
