WITH student_details AS (
    SELECT
        sd.id,
        sd.email,
        sd.first_name,
        sd.last_name,
        sd.phone,
        sd.caste,
        sd.annual_family_income_inr,
        sd.location_id
    FROM raw.student_details sd
),

student_cohort AS (
    SELECT DISTINCT
        student_id,
        cohort_code
    FROM dev_staging.stg_student_cohort
),

student_registration AS (
    SELECT
        student_id,
        form_details,
        form_details ->> 'new_college_name' AS new_college_name,
        form_details ->> 'new_university_name' AS new_university_name,
        form_details ->> 'currently_pursuing_year' AS currently_pursuing_year,
        form_details ->> 'partner_organization' AS partner_organization,
        form_details ->> 'motivation' AS motivation,
        form_details ->> 'problems' AS problems,
        form_details ->> 'student_category' AS student_category,
        registration_date,
        inserted_at,
        updated_at
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
    JOIN LATERAL unnest(se.subject_id) AS unnested_subject(subject_id)
        ON TRUE
    JOIN raw.subject_mapping sm
        ON unnested_subject.subject_id = sm.id
    JOIN raw.course_mapping cm
        ON se.education_course_id = cm.course_id
    LEFT JOIN raw.college_mapping colm
        ON se.college_id = colm.college_id
    LEFT JOIN raw.university_mapping um
        ON se.university_id = um.university_id
    WHERE
        (se.inserted_at IS NOT NULL OR se.updated_at IS NOT NULL)
),

aggregated_subjects AS (
    SELECT
        student_id,
        education_course_id,
        STRING_AGG(DISTINCT education_category, ', ') AS education_category,
        STRING_AGG(DISTINCT subject_area, ', ') AS subject_areas,
        STRING_AGG(DISTINCT sub_field, ', ') AS sub_fields_list
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
    sd.id AS student_id,
    sc.cohort_code,
    sd.email,
    sd.first_name,
    sd.last_name,
    sd.phone,
    sd.caste,
    sd.annual_family_income_inr,
    lm.state_union_territory,
    lm.district,
    lm.country,
    lm.city_category,
    asub.education_category,
    asub.subject_areas,
    asub.sub_fields_list,
    na.course_name,
    sr.currently_pursuing_year,
    na.college_name,
    na.university_name,
    sr.new_college_name,
    sr.new_university_name,
    sr.partner_organization,
    sr.motivation,
    sr.problems,
    sr.student_category,
    sr.form_details,
    sr.registration_date,
    sr.inserted_at,
    sr.updated_at

FROM student_details sd

INNER JOIN student_cohort sc
    ON sd.id::TEXT = sc.student_id

LEFT JOIN student_registration sr
    ON sd.id = sr.student_id

LEFT JOIN raw.location_mapping lm
    ON sd.location_id = lm.location_id

LEFT JOIN aggregated_subjects asub
    ON sd.id = asub.student_id

LEFT JOIN non_aggregated na
    ON sd.id = na.student_id
    AND asub.education_course_id = na.education_course_id

WHERE sr.registration_date::TIMESTAMP >= TIMESTAMP '2025-12-19 00:00:00'