WITH silver_health_cleansed as (
    SELECT
        INITCAP(TRIM(patient_name)) AS patient_name,
        CASE
            WHEN patient_age < 1 THEN NULL
            ELSE patient_age
        END as patient_age,
        CASE
            WHEN SUBSTRING(patient_gender, 1, 1) = 'M' THEN 'M'
            WHEN SUBSTRING(patient_gender, 1, 1) = 'F' THEN 'F' 
        END as patient_gender,
        blood_type,
        medical_condition,
        DATE(date_of_admission) as date_of_admission,
        INITCAP(TRIM(patient_doctor)) as patient_doctor,
        INITCAP(TRIM(hospital_name)) as hospital_name,
        INITCAP(TRIM(insurance_provider)) as insurance_provider,
        CAST(billing_amount AS DECIMAL(10, 2)) AS billing_amount,
        room_number,
        UPPER(admission_type) as admission_type,
        DATE(discharge_date) as discharge_date,
        INITCAP(TRIM(patient_medication)) as patient_medication,
        CASE 
            WHEN LOWER(patient_test_results) = 'normal' THEN 'NORMAL'  
            WHEN LOWER(patient_test_results) = 'abnormal' THEN 'ABNORMAL'  
            ELSE 'INCONCLUSIVE'
        END as patient_test_results,
        load_time
    FROM {{ ref('stg_silver_health') }}
),
 not_duplicates_cleaned as (
    Select *, ROW_NUMBER() OVER(
                PARTITION BY 
                patient_name, 
                patient_age, 
                blood_type, 
                hospital_name, 
                date_of_admission, 
                discharge_date
                ORDER BY date_of_admission ASC) as row_num
     FROM silver_health_cleansed
) 
Select * FROM not_duplicates_cleaned
WHERE row_num = 1










