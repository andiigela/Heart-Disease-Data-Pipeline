-- Group patients by age group and gender to analyze risk patterns.
/* te shikohen levelet, dhe numri i moshave dhe gjinive ne combined risk score te levelit perkates */
with gold_patient_cluster_risks AS (
    SELECT
        age_group,
        gender,
        ROUND(AVG(combined_risk_score), 2) AS avg_combined_risk_score,
        ROUND(MAX(combined_risk_score), 2) AS max_combined_risk_score,
        COUNT(CASE WHEN combined_risk_score_level = 'Critical' THEN 1 END) AS critical_risk_patients_count,
        COUNT(CASE WHEN combined_risk_score_level = 'Moderate' THEN 1 END) AS moderate_risk_patients_count,
        COUNT(CASE WHEN combined_risk_score_level = 'Low' THEN 1 END) AS low_risk_patients_count
    FROM {{ ref('fct_silver_local_health') }}
    GROUP BY age_group, gender
)
Select * from gold_patient_cluster_risks
