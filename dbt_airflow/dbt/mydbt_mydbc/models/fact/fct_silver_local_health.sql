/* normalization formula => X - Min/ Max - Min = Res   */
with fact_normalization as (
    Select 
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
) 
Select * FROM fact_normalization

