/* per me analizu perqindjet e rezultateve pozitive dhe negative duke u bazu ne gjini, moshe, dhe rrezikun total te shendetit */

with patient_risk_result_distribution AS (
    SELECT
        gender,
        age_group,
        combined_risk_score_level,
        COUNT(*) AS total_patients,
        ROUND(SUM(CASE WHEN patient_result = 'positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS positive_percentage,
        ROUND(SUM(CASE WHEN patient_result = 'negative' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS negative_percentage

    FROM {{ ref('fct_silver_local_health') }}
    GROUP BY gender, age_group, combined_risk_score_level
    ORDER BY positive_percentage, negative_percentage DESC
)
SELECT * FROM patient_risk_result_distribution