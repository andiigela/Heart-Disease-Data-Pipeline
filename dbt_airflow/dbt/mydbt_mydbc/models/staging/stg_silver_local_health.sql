with silver_local_health as (
    Select 
        Age as patient_age,
        Gender as patient_gender,
        Heart_rate as patient_heart_rate,
        Systolic_blood_pressure as patient_systolic_blood_pressure,
        Diastolic_blood_pressure as patient_diastolic_blood_pressure,
        Blood_sugar as patient_blood_sugar,
        CK_MB as ck_mb,
        Troponin as patient_troponin,
        Result as patient_result,
        load_time 
    FROM dev.demo_db.bronze_local_health
),
silver_local_health_null_checks AS (
  SELECT  DISTINCT * FROM silver_local_health
    WHERE patient_age IS NOT NULL
    AND patient_gender IS NOT NULL
    AND patient_heart_rate IS NOT NULL
    AND patient_systolic_blood_pressure IS NOT NULL
    AND patient_diastolic_blood_pressure IS NOT NULL
    AND patient_blood_sugar IS NOT NULL
    AND ck_mb IS NOT NULL
    AND patient_troponin IS NOT NULL
    AND patient_result IS NOT NULL
),
silver_local_health_filtered AS (
  SELECT * FROM silver_local_health_null_checks
    WHERE patient_heart_rate BETWEEN 30 AND 200
    AND patient_systolic_blood_pressure BETWEEN 50 AND 250
    AND patient_diastolic_blood_pressure BETWEEN 40 AND 150
    AND patient_blood_sugar BETWEEN 40 AND 500 
    AND ck_mb BETWEEN 0 AND 50
    AND patient_troponin BETWEEN 0 AND 10
)
Select * FROM silver_local_health_filtered
