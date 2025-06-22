-- Fraud Detection Analysis Queries

-- Create database
CREATE DATABASE IF NOT EXISTS fraud_detection;
USE fraud_detection;

-- 1. Daily fraud statistics
SELECT 
    processing_date,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent,
    ROUND(AVG(amount), 2) as avg_transaction_amount,
    ROUND(SUM(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 2) as total_fraud_amount
FROM transactions_processed
GROUP BY processing_date
ORDER BY processing_date DESC;

-- 2. Top users by fraud transactions
SELECT 
    user_id,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent,
    ROUND(AVG(amount), 2) as avg_amount,
    ROUND(SUM(amount), 2) as total_amount
FROM transactions_processed
GROUP BY user_id
HAVING SUM(is_fraud) > 0
ORDER BY fraud_transactions DESC
LIMIT 20;

-- 3. Merchant analysis
SELECT 
    merchant_id,
    merchant_category,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent,
    ROUND(AVG(amount), 2) as avg_amount
FROM transactions_processed
GROUP BY merchant_id, merchant_category
HAVING COUNT(*) >= 10
ORDER BY fraud_rate_percent DESC
LIMIT 20;

-- 4. Location-based fraud analysis
SELECT 
    location,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent,
    ROUND(AVG(amount), 2) as avg_amount
FROM transactions_processed
GROUP BY location
HAVING COUNT(*) >= 5
ORDER BY fraud_rate_percent DESC;

-- 5. Time-based fraud patterns
SELECT 
    hour_of_day,
    is_weekend,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent
FROM transactions_processed
GROUP BY hour_of_day, is_weekend
ORDER BY hour_of_day, is_weekend;

-- 6. Channel and card type analysis
SELECT 
    channel,
    card_type,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent,
    ROUND(AVG(amount), 2) as avg_amount
FROM transactions_processed
GROUP BY channel, card_type
ORDER BY fraud_rate_percent DESC;

-- 7. High-value transaction analysis
SELECT 
    CASE 
        WHEN amount < 100 THEN '< $100'
        WHEN amount < 500 THEN '$100 - $500'
        WHEN amount < 1000 THEN '$500 - $1000'
        WHEN amount < 5000 THEN '$1000 - $5000'
        ELSE '> $5000'
    END as amount_range,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent
FROM transactions_processed
GROUP BY 
    CASE 
        WHEN amount < 100 THEN '< $100'
        WHEN amount < 500 THEN '$100 - $500'
        WHEN amount < 1000 THEN '$500 - $1000'
        WHEN amount < 5000 THEN '$1000 - $5000'
        ELSE '> $5000'
    END
ORDER BY fraud_rate_percent DESC;

-- 8. Recent fraud alerts summary
SELECT 
    alert_type,
    severity,
    COUNT(*) as alert_count,
    COUNT(DISTINCT user_id) as affected_users,
    ROUND(AVG(amount), 2) as avg_amount
FROM fraud_alerts
WHERE processing_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY alert_type, severity
ORDER BY alert_count DESC;

-- 9. User behavior analysis
WITH user_behavior AS (
    SELECT 
        user_id,
        COUNT(*) as transaction_count,
        COUNT(DISTINCT merchant_id) as unique_merchants,
        COUNT(DISTINCT location) as unique_locations,
        ROUND(AVG(amount), 2) as avg_amount,
        ROUND(STDDEV(amount), 2) as amount_stddev,
        SUM(is_fraud) as fraud_count
    FROM transactions_processed
    WHERE processing_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY user_id
)
SELECT 
    CASE 
        WHEN transaction_count < 10 THEN 'Low Activity'
        WHEN transaction_count < 50 THEN 'Medium Activity'
        ELSE 'High Activity'
    END as activity_level,
    COUNT(*) as user_count,
    ROUND(AVG(avg_amount), 2) as avg_transaction_amount,
    SUM(fraud_count) as total_fraud_transactions,
    ROUND(SUM(fraud_count) * 100.0 / SUM(transaction_count), 2) as fraud_rate_percent
FROM user_behavior
GROUP BY 
    CASE 
        WHEN transaction_count < 10 THEN 'Low Activity'
        WHEN transaction_count < 50 THEN 'Medium Activity'
        ELSE 'High Activity'
    END;

-- 10. Fraud trend analysis (last 30 days)
SELECT 
    processing_date,
    COUNT(*) as total_transactions,
    SUM(is_fraud) as fraud_transactions,
    ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) as fraud_rate_percent,
    LAG(ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2)) OVER (ORDER BY processing_date) as prev_fraud_rate,
    ROUND(
        ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2) - 
        LAG(ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 2)) OVER (ORDER BY processing_date), 
        2
    ) as fraud_rate_change
FROM transactions_processed
WHERE processing_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY processing_date
ORDER BY processing_date;
