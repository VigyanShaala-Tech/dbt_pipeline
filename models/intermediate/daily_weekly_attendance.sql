{{ config(
    materialized='table'
) }}

SELECT
    ss.id,
    ss.student_id,
    ss.session_id,
    ss.duration_in_sec,
    ss.watched_on,

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
    sc.cohort_code,
    sc.is_leader,
    sc.cohort_enroll_date,

    /* Live Session Details */
    ls.session_name,
    ls.type AS session_type,
    ls.code AS session_code,
    ls.duration_in_sec AS session_duration_in_sec,
    ls.conducted_on

FROM {{ source('raw', 'student_session') }} ss

INNER JOIN {{ source('raw', 'student_details') }} sd
    ON ss.student_id = sd.id

LEFT JOIN {{ source('raw', 'live_session') }} ls
    ON ss.session_id = ls.id

LEFT JOIN {{ source('raw', 'student_cohort') }} sc
    ON ss.student_id = sc.student_id
   AND ls.cohort_code = sc.cohort_code