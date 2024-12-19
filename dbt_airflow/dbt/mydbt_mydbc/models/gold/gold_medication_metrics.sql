-- A table to track the number of uses of each medication in each hospital and calculate the average billing associated with each medication.
WITH medication_metrics AS (
    SELECT 
        hospital_name,
        patient_medication,
        COUNT(*) AS patients_total_usage_medication,
        AVG(billing_amount) AS avg_billing_per_medication,
        ROUND((COUNT(*) / SUM(DISTINCT medications_used_by_hospital)) * 100.0, 2) AS medication_usage_percentage 
    FROM {{ ref('fact_silver_health') }}
    GROUP by hospital_name, patient_medication
) SELECT * FROM medication_metrics