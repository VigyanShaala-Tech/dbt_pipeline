{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['student_id', 'session_id']
) }}

WITH raw_student_cohort_data AS (
    SELECT student_id, cohort_code
    FROM raw.student_cohort
),
student_details_data AS (
    SELECT id,email FROM raw.student_details
),
session_data AS (
    SELECT id AS session_id, session_name, cohort_code, code
    FROM raw.live_session
),
raw_student_session_info AS (
    SELECT
        "Email" AS email,
        "Session_Code" AS session_code,
        "Duration_in_secs" AS duration_in_sec,
        "watched_on" AS watched_on
    FROM old.student_session_information
    WHERE "Session_Code" LIKE 'SUK%' 
        OR "Session_Code" LIKE 'WS%'      
        OR "Session_Code" LIKE 'MC%'
),
student_live_session_cte AS (
    SELECT
        sd.id::INT AS student_id,
        s.session_id::INT AS session_id,
        ssi.duration_in_sec::INT AS duration_in_sec,
        ssi.watched_on::DATE AS watched_on
    FROM raw_student_session_info ssi
    INNER JOIN student_details_data sd ON ssi.email = sd.email                    
    INNER JOIN raw_student_cohort_data sc ON sd.id = sc.student_id
    INNER JOIN session_data s ON ssi.session_code = s.code AND sc.cohort_code = s.cohort_code
)

SELECT sls.*
FROM student_live_session_cte sls
INNER JOIN raw_student_cohort_data sc 
    ON sls.student_id = sc.student_id
WHERE sc.cohort_code IN ('INC007')   --Change as per cohorts that needs to be updated, will reduce runtime.  