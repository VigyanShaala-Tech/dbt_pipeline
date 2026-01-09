{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['student_id', 'resource_id']
) }}


WITH raw_student_cohort_data AS (
    SELECT student_id, cohort_code
    FROM raw.student_cohort
),
student_details_data AS (
    SELECT id,email FROM raw.student_details
),
resource_data AS (
    SELECT id AS resource_id, title, total_duration AS watchtime_in_min
    FROM raw.resource
),
raw_student_session_info AS (
    SELECT
        "Email" AS email,
        "Session_Code" AS session_code,
        "Duration_in_secs" AS watchtime_in_secs,
        "watched_on" AS watched_on
    FROM old.student_session_information
    WHERE "Session_Code" LIKE 'VID%'
           
),
student_pre_recorded_cte AS (
    SELECT
        sd.id::INT AS student_id,
        r.resource_id::INT AS resource_id,
        sc.cohort_code AS cohort_code,
        ssi.watchtime_in_secs::INT AS watchtime_in_sec,
        ssi.watched_on::DATE AS watched_at
    FROM raw_student_session_info ssi
    INNER JOIN student_details_data sd ON ssi.email = sd.email                    
    INNER JOIN raw_student_cohort_data sc ON sd.id = sc.student_id
    INNER JOIN resource_data r ON ssi.session_code = r.title
)

SELECT *
FROM student_pre_recorded_cte
WHERE cohort_code IN ('INC007','INC008','INC009')   --Change as per cohorts that needs to be updated, will reduce runtime.  