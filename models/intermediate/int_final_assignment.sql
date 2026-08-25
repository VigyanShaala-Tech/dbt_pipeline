{{
    config(
        materialized='table',
        tags=['intermediate', 'assignment']
    )
}}

with student_assignments as (

    select
        id,
        student_id,
        resource_id,
        mentor_id,
        cohort_code,
        submission_status,
        marks,
        max_marks,
        marks_pct,
        feedback_comments,
        submitted_at,
        assignment_file,
        source_system

    from {{ ref('stg_student_assignment') }}

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

    /* Assignment Details */
    sa.id,
    sa.student_id,
    sa.resource_id,
    sa.mentor_id,
    sa.cohort_code,
    sa.submission_status,
    sa.marks,
    sa.max_marks,
    sa.marks_pct,
    sa.feedback_comments,
    sa.submitted_at,
    sa.assignment_file,
    sa.source_system,

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

from student_assignments sa

inner join student_details sd
    on sa.student_id::text = sd.student_id::text