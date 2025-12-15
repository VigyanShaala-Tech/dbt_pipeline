{{ config(
  materialized='table'
) }}


WITH cohort_range AS (
    SELECT cohort_code, start_date, end_date
    FROM raw.cohort
),

live_sessions AS (
    SELECT 
        ls.id,
        ls.session_name,
        ls.cohort_code,
        ls.code,
        ls.conducted_on::date AS conducted_on
    FROM raw.live_session ls
),

student_attendance AS (
    SELECT 
        sd.student_id,
        CASE 
            WHEN sc.cohort_code LIKE 'INC%' THEN 
                'Incubator ' || (ltrim(substring(sc.cohort_code, 4), '0') || '.0')
            ELSE sc.cohort_code
        END AS "Incubator_Batch",
        sdet.location_id,
        ls.id AS session_id,               
        ls.session_name AS title,
        ls.code,
        ls.conducted_on,
        sd.duration_in_sec,
        COALESCE(sd.watched_on::date, ls.conducted_on) AS attended_on
    FROM raw.student_session sd
    JOIN raw.student_details sdet
        ON sd.student_id = sdet.id
    LEFT JOIN raw.student_cohort sc
        ON sd.id = sc.student_id
    LEFT JOIN cohort_range c 
        ON sc.cohort_code = c.cohort_code
    LEFT JOIN live_sessions ls
        ON sd.session_id = ls.id AND c.cohort_code = ls.cohort_code
),

student_registration AS (
    SELECT
        student_id
        --form_details
    FROM raw.student_registration_details_2
),

mapped_subjects AS (
    SELECT 
        se.student_id,
        se.education_course_id,
        cm.course_name,
        colm.standard_college_names AS college_name,
        um.standard_university_names AS university_name,
        sm.education_category,
        sm.subject_area,
        sm.sub_field
    FROM raw.student_education se
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id) ON TRUE
    JOIN raw.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN raw.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN raw.college_mapping colm
        ON se.college_id = colm.college_id
    LEFT JOIN raw.university_mapping um
        ON se.university_id = um.university_id
),

aggregated_subjects AS (
    SELECT
        student_id,
        education_course_id,
        string_agg(DISTINCT education_category, ', ') AS education_category,
        string_agg(DISTINCT subject_area, ', ') AS subject_areas,
        string_agg(DISTINCT sub_field, ', ') AS sub_fields_list
    FROM mapped_subjects
    GROUP BY student_id, education_course_id
),

non_aggregated AS (
    SELECT DISTINCT
        student_id,
        education_course_id,
        course_name,
        college_name,
        university_name
    FROM mapped_subjects
)

SELECT
    TRIM(TO_CHAR(sa.attended_on, 'Day')) AS weekday_name,  
    sa.student_id,
    sa.session_id,
    sa."Incubator_Batch",
    sa.title,
    sa.code,
    sa.conducted_on,
    sa.attended_on,
    sa.duration_in_sec,
    --sr.form_details,
    lm.state_union_territory,
    lm.district,
    lm.country,
    lm.city_category,
    asub.education_category,
    asub.subject_areas,
    asub.sub_fields_list,
    na.course_name,
    na.college_name,
    na.university_name
FROM student_attendance sa
LEFT JOIN student_registration sr
    ON sa.student_id = sr.student_id
LEFT JOIN raw.location_mapping lm
    ON sa.location_id = lm.location_id
LEFT JOIN aggregated_subjects asub
    ON sa.student_id = asub.student_id
LEFT JOIN non_aggregated na
    ON sa.student_id = na.student_id
   AND asub.education_course_id = na.education_course_id
   WHERE (split_part(sa."Incubator_Batch", ' ', 2))::numeric > 7