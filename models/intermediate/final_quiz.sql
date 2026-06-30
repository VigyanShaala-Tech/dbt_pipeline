{{ config(
    materialized='table'
) }}

SELECT
    sq.id,
    sq.student_id,
    sq.resource_id,
    sq.cohort_code,
    sq.marks,
    sq.max_marks,
    sq.reattempts,
    sq.attempted_at,

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

    /* Student Cohort */
    sc.student_code,
    sc.is_leader,
    sc.cohort_enroll_date,

    /* Resource Details */
    r.category AS resource_category,
    r.title AS resource_title,
    r.description AS resource_description,
    r.location AS resource_location,
    r.resource_link,
    r.is_video_resource,
    r.total_duration

FROM {{ source('raw', 'student_quiz') }} sq

INNER JOIN {{ source('raw', 'student_details') }} sd
    ON sq.student_id = sd.id

LEFT JOIN {{ source('raw', 'student_cohort') }} sc
    ON sq.student_id = sc.student_id
   AND sq.cohort_code = sc.cohort_code

LEFT JOIN {{ source('raw', 'resource') }} r
    ON sq.resource_id = r.id

INNER JOIN {{ source('raw', 'resource_cohort') }} rc
    ON rc.resource_id = sq.resource_id
   AND rc.cohort_code = sq.cohort_code