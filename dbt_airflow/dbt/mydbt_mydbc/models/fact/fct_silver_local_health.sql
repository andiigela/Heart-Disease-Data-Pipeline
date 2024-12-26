/* normalization formula => X - Min/ Max - Min = Res, values from 0-1  */
/* not including categorical data anymore */
/* risk_score 0.4 tregon correlacionin e kolones me rezultatin (rrezikshmerine), duke u bazu ne clinical knowledge or expert opinions 
to assign weights. 
    Heart Rate (0.34): Primary contributor to cardiac stress.
    Systolic BP (0.30): Critical hypertension indicator.
    Diastolic BP (0.15): Adds context to systolic BP.
    Troponin (0.14): Specific marker for myocardial injury.
    Blood Sugar (0.04): Long-term cardiovascular relevance.
    CK-MB (0.03): Secondary cardiac marker.

*/
with fact_normalization as (
    Select 
        *,
        (patient_heart_rate - MIN(patient_heart_rate) OVER()) / 
        (MAX(patient_heart_rate) OVER () - MIN(patient_heart_rate) OVER ()) AS normalized_patient_heart_rate,
        (patient_systolic_blood_pressure - MIN(patient_systolic_blood_pressure) OVER ()) / 
        (MAX(patient_systolic_blood_pressure) OVER () - MIN(patient_systolic_blood_pressure) OVER ()) AS normalized_systolic_bp,
        (patient_diastolic_blood_pressure - MIN(patient_diastolic_blood_pressure) OVER()) /
        (MAX(patient_diastolic_blood_pressure) OVER() - MIN(patient_diastolic_blood_pressure) OVER()) AS normalized_diastolic_bp,
        (patient_blood_sugar - MIN(patient_blood_sugar) OVER()) / 
        (MAX(patient_blood_sugar) OVER() - MIN(patient_blood_sugar) OVER()) AS normalized_blood_sugar,
        (ck_mb - MIN(ck_mb) OVER()) /
        (MAX(ck_mb) OVER() - MIN(ck_mb) OVER()) as normalized_ck_mb,
        (patient_troponin - MIN(patient_troponin) OVER()) /
        (MAX(patient_troponin) OVER() - MIN(patient_troponin) OVER()) as normalized_troponin
    FROM {{ ref('dim_silver_local_health') }}
),
categorical_to_numerical AS (
     Select 
        *,
        CASE 
            WHEN patient_ck_mb_level = 'Low' THEN 0
            WHEN patient_ck_mb_level = 'Moderate' THEN 1
            WHEN patient_ck_mb_level = 'Critical' THEN 2
            ELSE -1
        END AS patient_ck_mb_level_num,
        CASE
            WHEN patient_troponin_level = 'Low' THEN 0
            WHEN patient_troponin_level = 'Moderate' THEN 1
            WHEN patient_troponin_level = 'Critical' THEN 2
            ELSE -1
        END AS patient_troponin_level_num,
        CASE
            WHEN patient_diabetes_risk = 'Low' THEN 0
            WHEN patient_diabetes_risk = 'Moderate' THEN 1
            WHEN patient_diabetes_risk = 'Critical' THEN 2
            ELSE -1
        END AS patient_diabetes_risk_num,
         CASE
            WHEN patient_systolic_bp_level = 'Low' THEN 0
            WHEN patient_systolic_bp_level = 'Moderate' THEN 1
            WHEN patient_systolic_bp_level = 'Critical' THEN 2
            ELSE -1
        END AS patient_systolic_bp_level_num,
        CASE
            WHEN patient_diastolic_bp_level = 'Low' THEN 0
            WHEN patient_diastolic_bp_level = 'Moderate' THEN 1
            WHEN patient_diastolic_bp_level = 'Critical' THEN 2
            ELSE -1
        END AS patient_diastolic_bp_level_num,
         CASE
            WHEN patient_heart_rate_category = 'Low' THEN 0
            WHEN patient_heart_rate_category = 'Moderate' THEN 1
            WHEN patient_heart_rate_category = 'Critical' THEN 2
            ELSE -1
        END AS patient_heart_rate_category_num,
        CASE 
            WHEN patient_hypertension_risk = 'Low' THEN 0
            WHEN patient_hypertension_risk = 'Moderate' THEN 1
            WHEN patient_hypertension_risk = 'High' THEN 2
            WHEN patient_hypertension_risk = 'Critical' THEN 3
            ELSE -1
        END AS patient_hypertension_risk_num,
        CASE 
            WHEN patient_heart_stress_level = 'Low' THEN 0
            WHEN patient_heart_stress_level = 'Moderate' THEN 1
            WHEN patient_heart_stress_level = 'High' THEN 2
            WHEN patient_heart_stress_level = 'Critical' THEN 3
            ELSE -1
        END AS patient_heart_stress_level_num
    FROM fact_normalization
),
combined_heart_rate_risk_score AS (
    Select *,
        (0.34 * normalized_patient_heart_rate +
            0.30 * normalized_systolic_bp +
            0.15 * normalized_diastolic_bp +
            0.14 * normalized_troponin +
            0.04 * normalized_blood_sugar +
            0.03 * normalized_ck_mb) AS combined_risk_score,
        CASE 
            WHEN (0.34 * normalized_patient_heart_rate +
                0.30 * normalized_systolic_bp +
                0.15 * normalized_diastolic_bp +
                0.14 * normalized_troponin +
                0.04 * normalized_blood_sugar +
                0.03 * normalized_ck_mb) < 0.4 THEN 'Low'
            WHEN (0.34 * normalized_patient_heart_rate +
                0.30 * normalized_systolic_bp +
                0.15 * normalized_diastolic_bp +
                0.14 * normalized_troponin +
                0.04 * normalized_blood_sugar +
                0.03 * normalized_ck_mb) BETWEEN 0.4 AND 0.7 THEN 'Moderate'
            ELSE 'Critical'
        END AS combined_risk_score_level
     FROM categorical_to_numerical
)
Select  
        patient_age,
        age_group,
        ck_mb,
        normalized_ck_mb,
        patient_ck_mb_level_num,
        patient_troponin,
        normalized_troponin,
        patient_troponin_level_num,
        patient_blood_sugar,
        normalized_blood_sugar,
        patient_diabetes_risk_num,
        patient_systolic_blood_pressure,
        normalized_systolic_bp,
        patient_systolic_bp_level_num,
        patient_diastolic_blood_pressure,
        normalized_diastolic_bp,
        patient_diastolic_bp_level_num,
        patient_heart_rate,
        normalized_patient_heart_rate,
        patient_heart_rate_category_num,
        patient_hypertension_risk_num,
        patient_hypertension_risk,
        patient_heart_stress_level_num,
        patient_heart_stress_level,
        gender,
        patient_gender,
        combined_risk_score,
        combined_risk_score_level,
        patient_result,
        load_time
    FROM combined_heart_rate_risk_score
WHERE 1 = 1
{% if is_incremental() %}
    AND load_time > (select max(load_time) from {{ this }})
{% endif %}