-- A table to capture hospital admission trends, including daily admission counts and rolling averages for billing.
WITH admission_trends AS (
    Select
         hospital_name,
         length_of_stay_days,
         SUM(billing_amount) AS total_billing_per_stay_days,
         AVG(billing_amount) AS avg_billing_per_stay_days
    FROM {{ ref('fact_silver_health') }}
    GROUP BY hospital_name, length_of_stay_days
) SELECT * FROM admission_trends