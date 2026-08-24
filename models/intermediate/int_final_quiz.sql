{{
    config(
        materialized='table',
        tags=['intermediate', 'quiz']
    )
}}

with student_quiz as (

    select
        student_id,
        resource_id,
        cohort_code,
        marks,
        max_marks,
        reattempts,
        attempted_at,
        source_system

    from {{ ref('stg_student_quiz') }}

),

student_details as (

    select
        student_id,
        email,
        first_name,
        last_name,
        gender,
        phone,
        date_of_birth,
        caste,
        annual_family_income_inr,
        location_id

    from {{ ref('stg_student_details') }}

)

select

    /* Quiz Details */
    sq.student_id,
    sq.resource_id,
    sq.cohort_code,
    sq.marks,
    sq.max_marks,
    sq.reattempts,
    sq.attempted_at,
    sq.source_system,

    /* Student Details */
    sd.email,
    sd.first_name,
    sd.last_name,
    sd.gender,
    sd.phone,
    sd.date_of_birth,
    sd.caste,
    sd.annual_family_income_inr,
    sd.location_id

from student_quiz sq

inner join student_details sd
    on sq.student_id::text = sd.student_id::text