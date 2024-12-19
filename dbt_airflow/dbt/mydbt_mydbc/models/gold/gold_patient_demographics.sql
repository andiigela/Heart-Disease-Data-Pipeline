-- A table to capture hospital admission trends, including daily admission counts and rolling averages for billing.
WITH patient_demographics AS (
    SELECT 
        hospital_name,
        COUNT(*) AS total_patients,
        AVG(patient_age) as avg_patient_age,
        COUNT(CASE WHEN patient_gender = 'M' THEN 1 END) AS total_male_patients,
        COUNT(CASE WHEN patient_gender = 'F' THEN 1 END) AS total_female_patients,
        ROUND((COUNT(CASE WHEN patient_gender = 'M' THEN 1 END) / COUNT(*)) * 100.0, 2) AS male_patients_percentage,
        ROUND((COUNT(CASE WHEN patient_gender = 'F' THEN 1 END) / COUNT(*)) * 100.0, 2) AS female_patients_percentage
    FROM {{ ref('fact_silver_health') }}
    GROUP by hospital_name
) SELECT * FROM patient_demographics