with fact_admissions as (
    SELECT patient_name, patient_age, patient_gender, blood_type, medical_condition, date_of_admission,
        discharge_date, hospital_name, insurance_provider, billing_amount, room_number, admission_type,
        patient_medication, patient_test_results,
        COUNT(*) OVER(PARTITION BY hospital_name) as total_patients_in_hospital,
        DATEDIFF(discharge_date, date_of_admission) as length_of_stay_days,
        CASE
            WHEN patient_test_results = 'ABNORMAL' THEN 1
            WHEN patient_test_results = 'NORMAL' THEN 2
            ELSE 3
        END as patient_test_results_category,
        SUM(billing_amount) OVER(PARTITION BY hospital_name) AS total_billing_per_hospital,
        (SUM(CASE WHEN patient_test_results = 'ABNORMAL' THEN 1 ELSE 0 END) OVER (PARTITION BY hospital_name) / 
        COUNT(*) OVER (PARTITION BY hospital_name))* 100.0 as percent_abnormal_results,
        COUNT(patient_medication) OVER (PARTITION BY hospital_name) as medications_used_by_hospital,
        load_time
    FROM {{ ref('dim_silver_health') }}
) Select * from fact_admissions
WHERE 1 = 1
{% if is_incremental() %}
    AND load_time > (select max(load_time) from {{ this }})
{% endif %}
