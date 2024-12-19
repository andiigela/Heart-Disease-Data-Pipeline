-- A table to summarize key hospital-level metrics such as total admissions, total billing, and abnormal result percentages.
WITH hospital_metrics AS (
    Select hospital_name, 
        COUNT(*) AS total_patients,
        SUM(billing_amount) AS total_billing,
        AVG(billing_amount) AS avg_billing,
        MIN(length_of_stay_days) as min_length_of_stay_days,
        MAX(length_of_stay_days) as max_length_of_stay_days,
        AVG(length_of_stay_days) as avg_length_of_stay_days,
        SUM(DISTINCT percent_abnormal_results) as percent_abnormal_results
    FROM {{ ref('fact_silver_health') }}
    GROUP BY hospital_name
) SELECT * FROM hospital_metrics