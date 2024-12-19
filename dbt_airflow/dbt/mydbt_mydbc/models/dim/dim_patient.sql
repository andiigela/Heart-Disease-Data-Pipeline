WITH dim_patient AS (
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
        END AS patient_blood_sugar_level,
        CASE
            WHEN patient_systolic_blood_pressure < 90 THEN 'Low'
            WHEN patient_systolic_blood_pressure BETWEEN 90 AND 120 THEN 'Normal'
            WHEN patient_systolic_blood_pressure BETWEEN 121 AND 139 THEN 'Elevated'
            WHEN patient_systolic_blood_pressure BETWEEN 140 AND 179 THEN 'High'
            WHEN patient_systolic_blood_pressure >= 180 THEN 'Critical'
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
        END AS patient_heart_rate_category
    FROM {{ ref('stg_silver_local_health') }}
)
SELECT * FROM dim_patient;