/* Te gjindet mesataret e zemres dhe shtypjes se gjakut duke u bazuar ne risk score e kombinuar dhe stres levelin duke gjetur numrin e 
rasteve kritike te stres levelit te pacientit */
/* Helps build models to predict stress levels based on combined risk scores.
    Ju ndihmon modeleve te predikojne levele te stresit duke u bazuar ne score e pergjithshem shendetsore dhe levelit te stresit 
    te pacientit */
WITH stress_risk_interaction AS (
    SELECT
        combined_risk_score_level,
        patient_heart_stress_level,
        COUNT(*) AS total_patients,
        ROUND(AVG(normalized_patient_heart_rate), 2) AS avg_heart_rate,
        ROUND(AVG(normalized_systolic_bp), 2) AS avg_systolic_bp,
        ROUND(AVG(normalized_diastolic_bp), 2) AS avg_diastolic_bp,
        COUNT(CASE WHEN patient_heart_stress_level = 'Critical' THEN 1 END) AS high_stress_cases
    FROM {{ ref('fct_silver_local_health') }}
    GROUP BY combined_risk_score_level, patient_heart_stress_level
)
SELECT * FROM stress_risk_interaction;