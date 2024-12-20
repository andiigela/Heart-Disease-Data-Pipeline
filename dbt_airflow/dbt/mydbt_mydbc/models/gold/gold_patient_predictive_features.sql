/*  Provide aggregated metrics (average, min, max) for key health indicators grouped by age, group and gender */
/* Identifikon demografine qe ka vlera ekstreme per metrika specifike shendetesore. */
WITH predictive_features AS (
    SELECT 
        gender,
        age_group,
        ROUND(AVG(normalized_ck_mb), 2) AS avg_normalized_ck_mb,
        ROUND(MIN(normalized_ck_mb), 2) AS min_normalized_ck_mb,
        ROUND(MAX(normalized_ck_mb), 2) AS max_normalized_ck_mb,
        ROUND(AVG(normalized_troponin), 2) AS avg_normalized_troponin,
        ROUND(MIN(normalized_troponin), 2) AS min_normalized_troponin,
        ROUND(MAX(normalized_troponin), 2) AS max_normalized_troponin,
        ROUND(AVG(normalized_blood_sugar), 2) AS avg_normalized_blood_sugar,
        ROUND(MIN(normalized_blood_sugar), 2) AS min_normalized_blood_sugar,
        ROUND(MAX(normalized_blood_sugar), 2) AS max_normalized_blood_sugar,
        ROUND(AVG(normalized_systolic_bp), 2) AS avg_normalized_systolic_bp,
        ROUND(AVG(normalized_diastolic_bp), 2) AS avg_normalized_diastolic_bp,
        ROUND(AVG(normalized_patient_heart_rate), 2) AS avg_normalized_heart_rate,
        COUNT(*) AS total_patients
    FROM {{ ref('fct_silver_local_health') }}
    GROUP BY gender, age_group
)
SELECT * FROM predictive_features
