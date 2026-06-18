{{ config(
    materialized='table'
) }}

SELECT
    sa.id,
    sa.student_id,
    sa.resource_id,
    sa.mentor_id,
    sa.cohort_code,
    sa.submission_status,
    sa.marks_pct,
    sa.feedback_comments,
    sa.submitted_at,
    sa.assignment_file,

    /* Student Details */
    sd.email,
    sd.first_name,
    sd.last_name,
    sd.gender,
    sd.phone,
    sd.date_of_birth,
    sd.caste,
    sd.annual_family_income_inr,
    sd.location_id,


    /* Resource Details */
    r.category AS resource_category,
    r.title AS resource_title,
    r.description AS resource_description,
    r.location AS resource_location,
    r.resource_link,
    r.is_video_resource,
    r.total_duration

FROM {{ source('raw', 'student_assignment') }} sa

INNER JOIN {{ source('raw', 'student_details') }} sd
    ON sa.student_id = sd.id

LEFT JOIN {{ source('raw', 'student_cohort') }} sc
    ON sa.student_id = sc.student_id
   AND sa.cohort_code = sc.cohort_code

LEFT JOIN {{ source('raw', 'resource') }} r
    ON sa.resource_id = r.id