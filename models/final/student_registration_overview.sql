{{ config(
    materialized='table'
) }}

SELECT
    student_code,
    student_id,
    email,

    /* Full Name */
    TRIM(
        CONCAT(
            COALESCE(first_name, ''),
            ' ',
            COALESCE(last_name, '')
        )
    ) AS full_name,

    phone,
    cohort_code,
    is_leader,
    cohort_enroll_date,
    date_of_birth,
    caste,
    annual_family_income_inr,
    education_categories,
    subject_areas,
    sub_fields_list,

    interest_education_categories,
    interest_subject_areas,
    interest_sub_fields_list,
    course_name,

    CASE
        WHEN university_name IS NULL
             OR TRIM(university_name) = ''
        THEN new_university_name
        ELSE university_name
    END AS university_name,

    CASE
        WHEN college_name IS NULL
             OR TRIM(college_name) = ''
        THEN new_college_name
        ELSE college_name
    END AS college_name,

    registration_date,
    assigned_through,
    currently_pursuing_year,
    partner_organization,
    student_category,

    /* Location */
    country,
    state_union_territory,
    district,
    city_category,

    form_details,

    /* Audit Columns */
    student_inserted_at,
    student_updated_at,
    education_inserted_at,
    education_updated_at,
    registration_inserted_at,
    registration_updated_at

FROM {{ ref('student_demography') }}