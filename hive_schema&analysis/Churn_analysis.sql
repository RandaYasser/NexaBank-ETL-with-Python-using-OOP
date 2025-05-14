USE nexabank;

--  Step 1: Identify Churned Customers
WITH recent_activity AS (
  SELECT customer_id FROM credit_cards_billing WHERE payment_date >= '2025-02-01'
  UNION
  SELECT sender AS customer_id FROM transactions WHERE transaction_date >= '2025-02-01'
  UNION
  SELECT receiver AS customer_id FROM transactions WHERE transaction_date >= '2025-02-01'
),
churned_customers AS (
  SELECT DISTINCT customer_id
  FROM customer_profiles
  WHERE customer_id NOT IN (SELECT customer_id FROM recent_activity)
)
SELECT * FROM churned_customers;


-- 1. Churn by Age, Location, and Spending Level
-- Age and city churn rate
SELECT
  age,
  city,
  COUNT(DISTINCT cp.customer_id) AS total_customers,
  COUNT(DISTINCT cc.customer_id) AS churned_customers,
  ROUND(COUNT(DISTINCT cc.customer_id) * 100.0 / COUNT(DISTINCT cp.customer_id), 2) AS churn_rate
FROM customer_profiles cp
LEFT JOIN churned_customers cc ON cp.customer_id = cc.customer_id
GROUP BY age, city
ORDER BY churn_rate DESC;


-- 2. Churn Correlation with Late Credit Card Payments
WITH late_payments AS (
  SELECT customer_id
  FROM credit_cards_billing
  WHERE payment_date > month + INTERVAL 5 DAY  -- assuming payment is late if > 5 days late
),
churn_with_late AS (
  SELECT DISTINCT customer_id FROM churned_customers WHERE customer_id IN (SELECT customer_id FROM late_payments)
),
churn_without_late AS (
  SELECT DISTINCT customer_id FROM churned_customers WHERE customer_id NOT IN (SELECT customer_id FROM late_payments)
)
SELECT
  (SELECT COUNT(*) FROM churn_with_late) AS churn_with_late_payment,
  (SELECT COUNT(*) FROM churn_without_late) AS churn_without_late_payment;


-- 3. Compare Activity: Churned vs Non-Churned
WITH non_churned_customers AS (
  SELECT DISTINCT customer_id
  FROM customer_profiles
  WHERE customer_id NOT IN (SELECT customer_id FROM churned_customers)
)
-- Average transactions per user
SELECT
  status,
  AVG(transaction_count) AS avg_transactions,
  AVG(loan_count) AS avg_loans
FROM (
  SELECT 'churned' AS status, t.customer_id,
         COUNT(DISTINCT tr.transaction_date) AS transaction_count,
         COUNT(DISTINCT l.utilization_date) AS loan_count
  FROM churned_customers t
  LEFT JOIN transactions tr ON tr.sender = t.customer_id OR tr.receiver = t.customer_id
  LEFT JOIN loans l ON l.customer_id = t.customer_id
  GROUP BY t.customer_id

  UNION ALL

  SELECT 'non_churned' AS status, t.customer_id,
         COUNT(DISTINCT tr.transaction_date),
         COUNT(DISTINCT l.utilization_date)
  FROM non_churned_customers t
  LEFT JOIN transactions tr ON tr.sender = t.customer_id OR tr.receiver = t.customer_id
  LEFT JOIN loans l ON l.customer_id = t.customer_id
  GROUP BY t.customer_id
) activity
GROUP BY status;


-- 4. Spending Patterns Predicting Churn
SELECT
  churn_status,
  ROUND(AVG(amount_due), 2) AS avg_amount_due,
  ROUND(AVG(amount_paid), 2) AS avg_amount_paid
FROM (
  SELECT 'churned' AS churn_status, cb.*
  FROM credit_cards_billing cb
  JOIN churned_customers cc ON cb.customer_id = cc.customer_id

  UNION ALL

  SELECT 'non_churned' AS churn_status, cb.*
  FROM credit_cards_billing cb
  WHERE cb.customer_id NOT IN (SELECT customer_id FROM churned_customers)
) credit_data
GROUP BY churn_status;

-- 5. ARPU and High Spenders vs. Churners
-- Revenue = Total amount paid by each customer
WITH revenue_per_user AS (
  SELECT customer_id, SUM(amount_paid) AS total_revenue
  FROM credit_cards_billing
  GROUP BY customer_id
),
arpu_data AS (
  SELECT
    cp.customer_id,
    cp.age,
    cp.city,
    cp.customer_tier,
    rpu.total_revenue,
    CASE WHEN cc.customer_id IS NOT NULL THEN 'churned' ELSE 'active' END AS status
  FROM customer_profiles cp
  LEFT JOIN revenue_per_user rpu ON cp.customer_id = rpu.customer_id
  LEFT JOIN churned_customers cc ON cp.customer_id = cc.customer_id
)
SELECT status,
       ROUND(AVG(total_revenue), 2) AS arpu
FROM arpu_data
GROUP BY status;

-- 6. Churn by City
SELECT
  city,
  COUNT(DISTINCT cp.customer_id) AS total_customers,
  COUNT(DISTINCT cc.customer_id) AS churned_customers,
  ROUND(COUNT(DISTINCT cc.customer_id) * 100.0 / COUNT(DISTINCT cp.customer_id), 2) AS churn_rate
FROM customer_profiles cp
LEFT JOIN churned_customers cc ON cp.customer_id = cc.customer_id
GROUP BY city
ORDER BY churn_rate DESC;

-- 7. Tenure vs Churn
SELECT
  tenure_bucket,
  COUNT(DISTINCT cp.customer_id) AS total_customers,
  COUNT(DISTINCT cc.customer_id) AS churned_customers,
  ROUND(COUNT(DISTINCT cc.customer_id) * 100.0 / COUNT(DISTINCT cp.customer_id), 2) AS churn_rate
FROM (
  SELECT *,
    CASE
      WHEN months_between('2025-05-01', account_open_date) < 6 THEN '0-6 months'
      WHEN months_between('2025-05-01', account_open_date) BETWEEN 6 AND 12 THEN '6-12 months'
      ELSE '12+ months'
    END AS tenure_bucket
  FROM customer_profiles
) cp
LEFT JOIN churned_customers cc ON cp.customer_id = cc.customer_id
GROUP BY tenure_bucket
ORDER BY tenure_bucket;

