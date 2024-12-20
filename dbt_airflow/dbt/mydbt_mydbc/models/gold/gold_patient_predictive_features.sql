/* Aggregate feature statistics for ML feature engineering to predict patient risk levels. 
    Zbulim te trendeve qe do ti ndihmojne ML modeleve te fokusohen ne diferencat e grupeve kur te bejne prediksione te bazuar ne populacion
      ose ndonje rekomandim.
*/
WITH predictive_features AS (
    SELECT 
        gender,
        age_group,
        ROUND(AVG(normalized_patient_heart_rate), 2) AS avg_heart_rate,
        ROUND(AVG(normalized_systolic_bp), 2) AS avg_systolic_bp,
        ROUND(AVG(normalized_diastolic_bp), 2) AS avg_diastolic_bp,
        ROUND(AVG(normalized_troponin), 2) AS avg_troponin,
        ROUND(AVG(normalized_blood_sugar), 2) AS avg_blood_sugar,
        ROUND(AVG(normalized_ck_mb), 2) AS avg_ck_mb,
        ROUND(AVG(combined_risk_score), 2) AS avg_combined_risk_score,
        COUNT(*) AS total_patients
    FROM {{ ref('fct_silver_local_health') }}
    GROUP BY gender, age_group
)
SELECT * FROM predictive_features
