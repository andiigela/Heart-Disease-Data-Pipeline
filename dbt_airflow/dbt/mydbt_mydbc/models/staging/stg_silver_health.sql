with silver_health as (
    Select * from dev.demo_db.health_bronze
)
Select 
  Name as patient_name,
  Age as patient_age,
  Gender as patient_gender,
  Blood_Type as blood_type,
  Medical_Condition as medical_condition,
  Date_of_Admission as date_of_admission,
  Doctor as patient_doctor,
  Hospital as hospital_name,
  Insurance_Provider as insurance_provider,
  Billing_Amount as billing_amount,
  Room_Number as room_number,
  Admission_Type as admission_type,
  Discharge_Date as discharge_date,
  Medication as patient_medication,
  Test_Results as patient_test_results,
  load_time
 from silver_health