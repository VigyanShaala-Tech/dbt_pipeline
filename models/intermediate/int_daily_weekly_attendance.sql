{{ config(
    materialized='table'
) }}

SELECT
    ss.id,
    ss.student_id,
    ss.session_id,
    ss.duration_in_sec,
    ss.watched_on,
    ss.source_system AS attendance_source_system,

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
    ls.conducted_on,
    ls.source_system AS session_source_system

FROM {{ ref('stg_student_sessions') }} ss

INNER JOIN {{ ref('stg_student_details') }} sd
    ON ss.student_id::text = sd.student_id::text

LEFT JOIN {{ ref('stg_live_session') }} ls
    ON ss.session_id::text = ls.live_session_id::text

LEFT JOIN {{ ref('stg_student_cohort') }} sc
    ON ss.student_id::text = sc.student_id::text
   AND ls.cohort_code::text = sc.cohort_code::text