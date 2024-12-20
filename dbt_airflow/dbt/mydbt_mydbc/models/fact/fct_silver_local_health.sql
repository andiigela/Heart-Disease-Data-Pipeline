/* normalization formula => X - Min/ Max - Min = Res, values from 0-1  */
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
categorical_to_numerical_values AS (
    Select 
        *,
        CASE 
            WHEN patient_ck_mb_level = 'Low' THEN 0
            WHEN patient_ck_mb_level = 'Moderate' THEN 1
            WHEN patient_ck_mb_level = 'Critical' THEN 2
        END AS patient_ck_mb_level_num,
        CASE
            WHEN patient_troponin_level = 'Low' THEN 0
            WHEN patient_troponin_level = 'Moderate' THEN 1
            WHEN patient_troponin_level = 'Critical' THEN 2
        END AS patient_troponin_level_num,
        CASE
            WHEN patient_diabetes_risk = 'Low' THEN 0
            WHEN patient_diabetes_risk = 'Moderate' THEN 1
            WHEN patient_diabetes_risk = 'Critical' THEN 2
        END AS patient_diabetes_risk_num,
        CASE
            WHEN patient_systolic_bp_level = 'Low' THEN 0
            WHEN patient_systolic_bp_level = 'Moderate' THEN 1
            WHEN patient_systolic_bp_level = 'Critical' THEN 2
        END AS patient_systolic_bp_level_num,
        CASE
            WHEN patient_diastolic_bp_level = 'Low' THEN 0
            WHEN patient_diastolic_bp_level = 'Moderate' THEN 1
            WHEN patient_diastolic_bp_level = 'Critical' THEN 2
        END AS patient_diastolic_bp_level_num,
        CASE
            WHEN patient_heart_rate_category = 'Low' THEN 0
            WHEN patient_heart_rate_category = 'Moderate' THEN 1
            WHEN patient_heart_rate_category = 'Critical' THEN 2
        END AS patient_heart_rate_category_num,
        CASE 
            WHEN patient_hypertension_risk = 'Low' THEN 0
            WHEN patient_hypertension_risk = 'Moderate' THEN 1
            WHEN patient_hypertension_risk = 'High' THEN 2
            WHEN patient_hypertension_risk = 'Critical' THEN 3
        END AS patient_hypertension_risk_num,
        CASE 
            WHEN patient_heart_stress_level = 'Low' THEN 0
            WHEN patient_heart_stress_level = 'Moderate' THEN 1
            WHEN patient_heart_stress_level = 'High' THEN 2
            WHEN patient_heart_stress_level = 'Critical' THEN 3
        END AS patient_heart_stress_level_num 
    FROM fact_normalization
)
Select * from categorical_to_numerical_values
