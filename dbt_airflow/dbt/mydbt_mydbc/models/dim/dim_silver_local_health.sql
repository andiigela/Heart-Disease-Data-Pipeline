WITH dim_silver_local_health AS (
    SELECT
        *,
        CASE
            WHEN patient_age BETWEEN 0 AND 30 THEN 'Young'
            WHEN patient_age BETWEEN 31 AND 60 THEN 'Middle-aged'
            ELSE 'Elderly'
        END AS age_group,
        CASE
            WHEN patient_gender = 1 THEN 'Male'
            WHEN patient_gender = 0 THEN 'Female'
            ELSE 'Unknown'
        END AS gender,
        CASE 
            WHEN ck_mb < 3 THEN 'Low'
            WHEN ck_mb BETWEEN 3 AND 6 THEN 'Moderate'
            WHEN ck_mb > 6 THEN 'Critical'
            ELSE 'Unknown'
        END AS patient_ck_mb_level,
        CASE
            WHEN patient_troponin < 0.04 THEN 'Low'
            WHEN patient_troponin BETWEEN 0.04 AND 0.4 THEN 'Moderate'
            WHEN patient_troponin > 0.4 THEN 'Critical'
            ELSE 'Unknown'
        END AS patient_troponin_level,
        CASE
            WHEN patient_blood_sugar < 70 THEN 'Low'
            WHEN patient_blood_sugar BETWEEN 70 AND 140 THEN 'Moderate'
            WHEN patient_blood_sugar > 140 THEN 'Critical'
            ELSE 'Unknown'
        END AS patient_diabetes_risk,
        CASE
            WHEN patient_systolic_blood_pressure < 90 THEN 'Low'
            WHEN patient_systolic_blood_pressure BETWEEN 90 AND 140 THEN 'Moderate'
            WHEN patient_systolic_blood_pressure > 140 THEN 'Critical'
            ELSE 'Unknown'
        END AS patient_systolic_bp_level,
       CASE
            WHEN patient_diastolic_blood_pressure < 60 THEN 'Low'
            WHEN patient_diastolic_blood_pressure BETWEEN 60 AND 90 THEN 'Moderate'
            WHEN patient_diastolic_blood_pressure > 90 THEN 'Critical'
            ELSE 'Unknown'
        END AS patient_diastolic_bp_level,
        CASE
            WHEN patient_heart_rate < 60 THEN 'Low'
            WHEN patient_heart_rate BETWEEN 60 AND 100 THEN 'Moderate'
            WHEN patient_heart_rate > 100 THEN 'Critical'
            ELSE 'Unknown'
        END AS patient_heart_rate_category,
        CASE 
            WHEN patient_systolic_blood_pressure < 120 AND patient_diastolic_blood_pressure < 80 THEN 'Low'
            WHEN (patient_systolic_blood_pressure BETWEEN 120 AND 139 OR patient_diastolic_blood_pressure BETWEEN 80 AND 89) THEN 'Moderate'
            WHEN (patient_systolic_blood_pressure BETWEEN 140 AND 159 OR patient_diastolic_blood_pressure BETWEEN 90 AND 99) THEN 'High'
            WHEN (patient_systolic_blood_pressure >= 160 OR patient_diastolic_blood_pressure >= 100) THEN 'Critical'
            ELSE 'Unknown'
        END AS patient_hypertension_risk,
        CASE 
            WHEN patient_heart_rate BETWEEN 60 AND 100 
                AND patient_systolic_blood_pressure BETWEEN 90 AND 120 
                AND patient_diastolic_blood_pressure BETWEEN 60 AND 80 
            THEN 'Low'
        WHEN (patient_heart_rate BETWEEN 101 AND 120 OR patient_systolic_blood_pressure BETWEEN 121 AND 140 OR patient_diastolic_blood_pressure BETWEEN 81 AND 90) THEN 'Moderate'
        WHEN (patient_heart_rate > 120 OR patient_systolic_blood_pressure BETWEEN 141 AND 180 OR patient_diastolic_blood_pressure BETWEEN 91 AND 110) THEN 'High'
        WHEN (patient_heart_rate > 180 OR patient_systolic_blood_pressure > 180 OR patient_diastolic_blood_pressure > 110) THEN 'Critical'
        ELSE 'Unknown'
        END AS patient_heart_stress_level,
        patient_result
    FROM {{ ref('stg_silver_local_health') }}
)
SELECT * FROM dim_silver_local_health;