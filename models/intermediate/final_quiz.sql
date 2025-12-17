{{ config(
  materialized='table'
) }}

WITH student_quiz AS (
    SELECT
        sd.id,
        sd.student_id,
        sd.cohort_code,
        sd.resource_id,
        sd.marks,
        sd.max_marks,
        r.category,
        r.title,
        CASE 
            WHEN sc.cohort_code LIKE 'INC%' THEN 
                'Incubator ' || (ltrim(substring(sc.cohort_code, 4), '0') || '.0')
            ELSE sc.cohort_code
        END AS "Incubator_Batch",
        sds.location_id
    FROM raw.student_quiz sd
    JOIN raw.student_details sds
        ON sd.student_id = sds.id
    LEFT JOIN raw.student_cohort sc
        ON sd.student_id = sc.student_id
    JOIN raw.resource r
        ON sd.resource_id = r.id
    
),
student_registration AS (
    SELECT student_id, form_details
    FROM raw.student_registration_details
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
    ss.id,
    ss.resource_id,
    ss.student_id,
    ss."Incubator_Batch",
    ss.category,
    ss.title,
    ss.cohort_code,
    ss.marks,
    ss.max_marks,
    sr.form_details,
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
FROM student_quiz ss
LEFT JOIN student_registration sr
    ON ss.student_id = sr.student_id
LEFT JOIN raw.location_mapping lm
    ON ss.location_id = lm.location_id
LEFT JOIN aggregated_subjects asub
    ON ss.student_id = asub.student_id
LEFT JOIN non_aggregated na
    ON ss.student_id = na.student_id
    AND asub.education_course_id = na.education_course_id