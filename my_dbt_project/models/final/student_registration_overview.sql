{{ config(
  materialized='table'
) }}


WITH student_demography AS (
    SELECT 
        student_id,
        email,
        caste,
        annual_family_income_inr,
        "Incubator_Batch",
        state_union_territory,
        district,
        country,
        city_category,
        form_details,
        education_category,
        subject_areas,
        sub_fields_list,
        course_name,
        college_name,
        university_name
    FROM intermediate.student_demography
),
student_registration_details AS (
    SELECT 
        srd.id,
        srd.student_id,
        srd.assigned_through,
        srd.registration_date::TIMESTAMP AS registration_date
    FROM raw.student_registration_details srd
),
student_details AS (
    SELECT 
        sd.id,
        sd.email,
        sd.phone
    FROM raw.student_details sd
)
SELECT
    sdm.student_id,
    sdm.email,
    sd.phone,
    sdm.caste,
    sdm.annual_family_income_inr,
    sdm."Incubator_Batch",
    sdm.state_union_territory,
    sdm.district,
    sdm.country,
    sdm.city_category,
    sdm.form_details,
    sdm.education_category,
    sdm.subject_areas,
    sdm.sub_fields_list,
    sdm.course_name,
    sdm.college_name,
    sdm.university_name,
    srd.registration_date
FROM student_demography sdm
INNER JOIN student_details sd ON sdm.student_id = sd.id
LEFT JOIN student_registration_details srd ON sd.id = srd.student_id