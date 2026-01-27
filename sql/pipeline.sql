SELECT current_database(), current_user, now();

CREATE TABLE IF NOT EXISTS raw_policies (
  policy_id TEXT,
  issue_date DATE,
  state TEXT,
  product TEXT,
  face_amount NUMERIC,
  annual_premium NUMERIC,
  policyholder_id TEXT,
  status TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_premium_payments (
  policy_id TEXT,
  payment_date DATE,
  amount NUMERIC,
  payment_method TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_claims (
  claim_id TEXT,
  policy_id TEXT,
  loss_date DATE,
  reported_date DATE,
  paid_amount NUMERIC,
  cause TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name LIKE 'raw_%'
ORDER BY table_name;



CREATE TABLE IF NOT EXISTS raw_policies (
  policy_id TEXT,
  issue_date DATE,
  state TEXT,
  product TEXT,
  face_amount NUMERIC,
  annual_premium NUMERIC,
  policyholder_id TEXT,
  status TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw_premium_payments (
  policy_id TEXT,
  payment_date DATE,
  amount NUMERIC,
  payment_method TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name LIKE 'raw_%'
ORDER BY table_name;


CREATE TABLE IF NOT EXISTS raw_policies (
  policy_id TEXT,
  issue_date DATE,
  state TEXT,
  product TEXT,
  face_amount NUMERIC,
  annual_premium NUMERIC,
  policyholder_id TEXT,
  status TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now()
);


SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name LIKE 'raw_%'
ORDER BY table_name;

SELECT 'raw_policies' AS table_name, COUNT(*) AS n FROM raw_policies
UNION ALL SELECT 'raw_premium_payments', COUNT(*) FROM raw_premium_payments
UNION ALL SELECT 'raw_claims', COUNT(*) FROM raw_claims
ORDER BY table_name;

SELECT COUNT(*) FROM raw_policies;
SELECT COUNT(*) FROM raw_premium_payments;
SELECT COUNT(*) FROM raw_claims;

SELECT COUNT(*) FROM raw_policies;
SELECT COUNT(*) FROM raw_premium_payments;


-- 1) Staging: policies (dedupe + basic normalization)
DROP TABLE IF EXISTS stg_policies;
CREATE TABLE stg_policies AS
SELECT DISTINCT
  policy_id,
  issue_date,
  upper(state) AS state,
  product,
  face_amount,
  annual_premium,
  policyholder_id,
  initcap(status) AS status,
  ingested_at
FROM raw_policies;

-- 2) Staging: premiums (flag bad rows)
DROP TABLE IF EXISTS stg_premium_payments;
CREATE TABLE stg_premium_payments AS
SELECT
  policy_id,
  payment_date,
  amount,
  payment_method,
  ingested_at,
  CASE
    WHEN amount IS NULL THEN 'missing_amount'
    WHEN amount < 0 THEN 'negative_amount'
    WHEN payment_date IS NULL THEN 'missing_payment_date'
    ELSE NULL
  END AS dq_issue
FROM raw_premium_payments;

-- 3) Staging: claims (flag bad rows, including unknown policy)
DROP TABLE IF EXISTS stg_claims;
CREATE TABLE stg_claims AS
SELECT
  c.claim_id,
  c.policy_id,
  c.loss_date,
  c.reported_date,
  c.paid_amount,
  c.cause,
  c.ingested_at,
  CASE
    WHEN c.paid_amount IS NULL THEN 'missing_paid_amount'
    WHEN c.paid_amount < 0 THEN 'negative_paid_amount'
    WHEN p.policy_id IS NULL THEN 'unknown_policy_id'
    WHEN c.loss_date < p.issue_date THEN 'loss_before_issue'
    ELSE NULL
  END AS dq_issue
FROM raw_claims c
LEFT JOIN stg_policies p
  ON c.policy_id = p.policy_id;


DROP TABLE IF EXISTS stg_policies;
CREATE TABLE stg_policies AS
SELECT DISTINCT
  policy_id,
  issue_date,
  upper(state) AS state,
  product,
  face_amount,
  annual_premium,
  policyholder_id,
  initcap(status) AS status,
  ingested_at
FROM raw_policies;




DROP TABLE IF EXISTS stg_premium_payments;
CREATE TABLE stg_premium_payments AS
SELECT
  policy_id,
  payment_date,
  amount,
  payment_method,
  ingested_at,
  CASE
    WHEN amount IS NULL THEN 'missing_amount'
    WHEN amount < 0 THEN 'negative_amount'
    WHEN payment_date IS NULL THEN 'missing_payment_date'
    ELSE NULL
  END AS dq_issue
FROM raw_premium_payments;

DROP TABLE IF EXISTS stg_claims;
CREATE TABLE stg_claims AS
SELECT
  c.claim_id,
  c.policy_id,
  c.loss_date,
  c.reported_date,
  c.paid_amount,
  c.cause,
  c.ingested_at,
  CASE
    WHEN c.paid_amount IS NULL THEN 'missing_paid_amount'
    WHEN c.paid_amount < 0 THEN 'negative_paid_amount'
    WHEN p.policy_id IS NULL THEN 'unknown_policy_id'
    WHEN c.loss_date < p.issue_date THEN 'loss_before_issue'
    ELSE NULL
  END AS dq_issue
FROM raw_claims c
LEFT JOIN stg_policies p
  ON c.policy_id = p.policy_id;



SELECT COUNT(*) FROM stg_policies;
SELECT COUNT(*) FROM stg_premium_payments;
SELECT COUNT(*) FROM stg_claims;




SELECT 'premiums' AS dataset, dq_issue, COUNT(*) AS n
FROM stg_premium_payments
WHERE dq_issue IS NOT NULL
GROUP BY 1,2
UNION ALL
SELECT 'claims', dq_issue, COUNT(*)
FROM stg_claims
WHERE dq_issue IS NOT NULL
GROUP BY 1,2
ORDER BY dataset, n DESC;




DROP TABLE IF EXISTS mart_policy_performance;
CREATE TABLE mart_policy_performance AS
WITH prem AS (
  SELECT
    policy_id,
    SUM(amount) AS total_premiums,
    COUNT(*) AS n_payments
  FROM stg_premium_payments
  WHERE dq_issue IS NULL
  GROUP BY 1
),
clm AS (
  SELECT
    policy_id,
    SUM(paid_amount) AS total_claims,
    COUNT(*) AS n_claims
  FROM stg_claims
  WHERE dq_issue IS NULL
  GROUP BY 1
)
SELECT
  p.policy_id,
  p.issue_date,
  p.state,
  p.product,
  p.face_amount,
  p.annual_premium,
  p.status,
  COALESCE(prem.total_premiums, 0) AS total_premiums,
  COALESCE(prem.n_payments, 0) AS n_payments,
  COALESCE(clm.total_claims, 0) AS total_claims,
  COALESCE(clm.n_claims, 0) AS n_claims,
  CASE
    WHEN COALESCE(prem.total_premiums, 0) = 0 THEN NULL
    ELSE ROUND(COALESCE(clm.total_claims, 0) / prem.total_premiums, 4)
  END AS loss_ratio
FROM stg_policies p
LEFT JOIN prem ON prem.policy_id = p.policy_id
LEFT JOIN clm  ON clm.policy_id  = p.policy_id;



DROP TABLE IF EXISTS mart_monthly_trends;
CREATE TABLE mart_monthly_trends AS
WITH prem_m AS (
  SELECT
    date_trunc('month', payment_date)::date AS month,
    SUM(amount) AS premiums
  FROM stg_premium_payments
  WHERE dq_issue IS NULL
  GROUP BY 1
),
clm_m AS (
  SELECT
    date_trunc('month', reported_date)::date AS month,
    SUM(paid_amount) AS claims
  FROM stg_claims
  WHERE dq_issue IS NULL
  GROUP BY 1
)
SELECT
  COALESCE(prem_m.month, clm_m.month) AS month,
  COALESCE(prem_m.premiums, 0) AS premiums,
  COALESCE(clm_m.claims, 0) AS claims,
  CASE
    WHEN COALESCE(prem_m.premiums, 0) = 0 THEN NULL
    ELSE ROUND(COALESCE(clm_m.claims, 0) / prem_m.premiums, 4)
  END AS loss_ratio
FROM prem_m
FULL OUTER JOIN clm_m USING (month)
ORDER BY month;



SELECT COUNT(*) FROM mart_policy_performance;

SELECT * FROM mart_policy_performance ORDER BY loss_ratio DESC NULLS LAST LIMIT 10;

SELECT * FROM mart_monthly_trends ORDER BY month DESC LIMIT 12;


SELECT 'raw_policies' AS table_name, COUNT(*) AS n FROM raw_policies
UNION ALL
SELECT 'raw_premium_payments', COUNT(*) FROM raw_premium_payments
UNION ALL
SELECT 'raw_claims', COUNT(*) FROM raw_claims
ORDER BY table_name;

SELECT 'premiums' AS dataset, dq_issue, COUNT(*) AS n
FROM stg_premium_payments
WHERE dq_issue IS NOT NULL
GROUP BY 1,2
UNION ALL
SELECT 'claims', dq_issue, COUNT(*)
FROM stg_claims
WHERE dq_issue IS NOT NULL
GROUP BY 1,2
ORDER BY dataset, n DESC;

SELECT *
FROM stg_premium_payments
WHERE dq_issue IS NOT NULL
ORDER BY ingested_at DESC
LIMIT 25;


SELECT *
FROM stg_claims
WHERE dq_issue IS NOT NULL
ORDER BY ingested_at DESC
LIMIT 25;


DROP TABLE IF EXISTS mart_policy_performance;
CREATE TABLE mart_policy_performance AS
WITH prem AS (
  SELECT
    policy_id,
    SUM(amount) AS total_premiums,
    COUNT(*) AS n_payments
  FROM stg_premium_payments
  WHERE dq_issue IS NULL
  GROUP BY 1
),
clm AS (
  SELECT
    policy_id,
    SUM(paid_amount) AS total_claims,
    COUNT(*) AS n_claims
  FROM stg_claims
  WHERE dq_issue IS NULL
  GROUP BY 1
)
SELECT
  p.policy_id,
  p.issue_date,
  p.state,
  p.product,
  p.face_amount,
  p.annual_premium,
  p.status,
  COALESCE(prem.total_premiums, 0) AS total_premiums,
  COALESCE(prem.n_payments, 0) AS n_payments,
  COALESCE(clm.total_claims, 0) AS total_claims,
  COALESCE(clm.n_claims, 0) AS n_claims,
  CASE
    WHEN COALESCE(prem.total_premiums, 0) = 0 THEN NULL
    ELSE ROUND(COALESCE(clm.total_claims, 0) / prem.total_premiums, 4)
  END AS loss_ratio
FROM stg_policies p
LEFT JOIN prem ON prem.policy_id = p.policy_id
LEFT JOIN clm  ON clm.policy_id  = p.policy_id;


SELECT
  policy_id,
  product,
  state,
  status,
  total_premiums,
  total_claims,
  loss_ratio
FROM mart_policy_performance
WHERE total_premiums > 0
ORDER BY loss_ratio DESC NULLS LAST
LIMIT 15;

SELECT COUNT(*) AS n_policies_in_mart
FROM mart_policy_performance;


SELECT
  policy_id,
  product,
  state,
  status,
  total_premiums,
  total_claims,
  loss_ratio
FROM mart_policy_performance
WHERE total_premiums > 0
ORDER BY loss_ratio DESC NULLS LAST
LIMIT 15;

DROP TABLE IF EXISTS mart_monthly_trends;
CREATE TABLE mart_monthly_trends AS
WITH prem_m AS (
  SELECT
    date_trunc('month', payment_date)::date AS month,
    SUM(amount) AS premiums
  FROM stg_premium_payments
  WHERE dq_issue IS NULL
  GROUP BY 1
),
clm_m AS (
  SELECT
    date_trunc('month', reported_date)::date AS month,
    SUM(paid_amount) AS claims
  FROM stg_claims
  WHERE dq_issue IS NULL
  GROUP BY 1
)
SELECT
  COALESCE(prem_m.month, clm_m.month) AS month,
  COALESCE(prem_m.premiums, 0) AS premiums,
  COALESCE(clm_m.claims, 0) AS claims,
  CASE
    WHEN COALESCE(prem_m.premiums, 0) = 0 THEN NULL
    ELSE ROUND(COALESCE(clm_m.claims, 0) / prem_m.premiums, 4)
  END AS loss_ratio
FROM prem_m
FULL OUTER JOIN clm_m USING (month)
ORDER BY month;


SELECT *
FROM mart_monthly_trends
ORDER BY month DESC
LIMIT 12;


