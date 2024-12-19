with silver_local_health as (
    Select * from dev.demo_db.bronze_local_health
)
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
 from silver_local_health