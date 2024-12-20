/* Te gjindet avg, min, max te vlerave te shtypjes se gjakut si (systolic, diastolic), dhe mesataren e rrahjes se zemres, duke numruar
te gjitha rasteet kritike te hipertensionit */
/* Ju ndihmon modeleve te predikojne levele te hipertensionit duke u bazuar ne score e pergjithshem shendetsore dhe levelit te 
    hipertensionit te pacientit */

WITH hypertension_risk_interaction AS (
    SELECT
        combined_risk_score_level,
        patient_hypertension_risk,
        COUNT(*) AS total_patients,

        ROUND(AVG(normalized_systolic_bp), 2) AS avg_systolic_bp, 
        ROUND(AVG(normalized_diastolic_bp), 2) AS avg_diastolic_bp,
        ROUND(MIN(normalized_systolic_bp), 2) AS min_systolic_bp,
        ROUND(MAX(normalized_systolic_bp), 2) AS max_systolic_bp,

        ROUND(AVG(normalized_patient_heart_rate), 2) AS avg_heart_rate,

        COUNT(CASE WHEN patient_hypertension_risk_num = 3 THEN 1 END) AS critical_hypertension_cases
    FROM {{ ref('fct_silver_local_health') }}
    GROUP BY combined_risk_score_level, patient_hypertension_risk
)
SELECT * FROM hypertension_risk_interaction;
