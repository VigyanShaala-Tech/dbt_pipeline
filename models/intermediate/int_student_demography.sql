{{
    config(
        materialized='table',
        tags=['intermediate', 'student_demography']
    )
}}

WITH cohort_students AS (

    SELECT
        student_code::text AS student_code,
        student_id::text AS student_id,
        cohort_code::text AS cohort_code,
        is_leader,
        cohort_enroll_date,
        source_system

    FROM {{ ref('stg_student_cohort') }}

),

student_details AS (

    SELECT *
    FROM (

        SELECT
            sd.*,

            ROW_NUMBER() OVER (
                PARTITION BY
                    sd.student_id,
                    sd.source_system

                ORDER BY
                    sd.updated_at DESC NULLS LAST,
                    sd.inserted_at DESC NULLS LAST

            ) AS rn

        FROM {{ ref('stg_student_details') }} sd

    ) ranked

    WHERE rn = 1

),

latest_education AS (

    SELECT *
    FROM (

        SELECT
            cs.student_id,
            cs.cohort_code,
            cs.source_system AS cohort_source_system,

            se.id,
            se.education_course_id,
            se.subject_id,
            se.interest_subject_id,
            se.college_id,
            se.university_id,
            se.college_location_id,

            se.start_year,
            se.end_year,

            se.course_name_raw,
            se.education_category_raw,
            se.subject_area_raw,
            se.sub_field_raw,

            se.interest_education_category_raw,
            se.interest_subject_area_raw,
            se.interest_sub_field_raw,

            se.college_name_raw,
            se.university_name_raw,

            se.country_raw,
            se.state_union_territory_raw,
            se.district_raw,
            se.city_category_raw,

            se.inserted_at,
            se.updated_at,
            se.source_system,

            ROW_NUMBER() OVER (

                PARTITION BY
                    cs.student_id,
                    cs.cohort_code,
                    cs.source_system

                ORDER BY

                    COALESCE(
                        se.updated_at,
                        se.inserted_at
                    ) DESC NULLS LAST,

                    CASE
                        WHEN se.source_system = 'OSOT' THEN 1
                        ELSE 2
                    END,

                    se.id DESC NULLS LAST

            ) AS rn

        FROM cohort_students cs

        INNER JOIN {{ ref('stg_student_education') }} se
            ON cs.student_id = se.student_id
           AND cs.source_system = se.source_system

    ) ranked_education

    WHERE rn = 1

),

latest_registration AS (

    SELECT *
    FROM (

        SELECT
            cs.student_id,
            cs.cohort_code,
            cs.source_system AS cohort_source_system,

            sr.registration_id,
            sr.assigned_through,
            sr.form_details,

            sr.registration_date,
            sr.inserted_at,
            sr.updated_at,

            sr.source_system,

            ROW_NUMBER() OVER (

                PARTITION BY
                    cs.student_id,
                    cs.cohort_code,
                    cs.source_system

                ORDER BY

                    COALESCE(
                        sr.updated_at,
                        sr.inserted_at
                    ) DESC NULLS LAST,

                    CASE
                        WHEN sr.source_system = 'OSOT' THEN 1
                        ELSE 2
                    END,

                    sr.registration_id DESC NULLS LAST

            ) AS rn

        FROM cohort_students cs

        INNER JOIN {{ ref('stg_student_registration_details') }} sr
            ON cs.student_id = sr.student_id
           AND cs.source_system = sr.source_system

    ) ranked_registration

    WHERE rn = 1

),

final AS (

    SELECT
        cs.student_code,
        cs.student_id,
        cs.cohort_code,
        cs.is_leader,
        cs.cohort_enroll_date,
        sd.student_id AS student_detail_id,

        sd.email,
        sd.first_name,
        sd.last_name,
        sd.gender,
        sd.phone,

        sd.date_of_birth,
        sd.caste,
        sd.annual_family_income_inr,

        sd.location_id,

        sd.inserted_at AS student_inserted_at,
        sd.updated_at AS student_updated_at,

        sd.source_system AS student_source_system,
        se.id AS education_id,

        se.education_course_id,
        se.subject_id,
        se.interest_subject_id,

        se.college_id,
        se.university_id,
        se.college_location_id,

        se.start_year,
        se.end_year,

        se.inserted_at AS education_inserted_at,
        se.updated_at AS education_updated_at,

        se.source_system AS education_source_system,

        se.course_name_raw AS course_name,

        se.education_category_raw
            AS education_categories,

        se.subject_area_raw
            AS subject_areas,

        se.sub_field_raw
            AS sub_fields_list,

        se.interest_education_category_raw
            AS interest_education_categories,

        se.interest_subject_area_raw
            AS interest_subject_areas,

        se.interest_sub_field_raw
            AS interest_sub_fields_list,

        se.college_name_raw
            AS college_name,

        se.university_name_raw
            AS university_name,
        sr.form_details::jsonb ->> 'new_college_name'
            AS new_college_name,

        sr.form_details::jsonb ->> 'new_university_name'
            AS new_university_name,

        COALESCE(
            NULLIF(TRIM(se.college_name_raw), ''),
            NULLIF(
                TRIM(sr.form_details::jsonb ->> 'new_college_name'),
                ''
            )
        ) AS final_college_name,

        COALESCE(
            NULLIF(TRIM(se.university_name_raw), ''),
            NULLIF(
                TRIM(sr.form_details::jsonb ->> 'new_university_name'),
                ''
            )
        ) AS final_university_name,

        sr.registration_id,

        sr.registration_date,
        sr.assigned_through,
        

        sr.form_details::jsonb ->> 'currently_pursuing_year'
            AS currently_pursuing_year,

        sr.form_details::jsonb ->> 'partner_organization'
            AS partner_organization,

        sr.form_details::jsonb ->> 'student_category'
            AS student_category,

        sr.form_details,

        sr.inserted_at AS registration_inserted_at,
        sr.updated_at AS registration_updated_at,

        sr.source_system AS registration_source_system,

        COALESCE(
            se.country_raw,
            sd.country_raw
        ) AS country,

        COALESCE(
            se.state_union_territory_raw,
            sd.state_union_territory_raw
        ) AS state_union_territory,

        COALESCE(
            se.district_raw,
            sd.district_raw
        ) AS district,

        COALESCE(
            se.city_category_raw,
            sd.city_category_raw
        ) AS city_category


    FROM cohort_students cs

    INNER JOIN student_details sd
        ON cs.student_id = sd.student_id
       AND cs.source_system = sd.source_system

    LEFT JOIN latest_education se
        ON cs.student_id = se.student_id
       AND cs.cohort_code = se.cohort_code
       AND cs.source_system = se.cohort_source_system

    LEFT JOIN latest_registration sr
        ON cs.student_id = sr.student_id
       AND cs.cohort_code = sr.cohort_code
       AND cs.source_system = sr.cohort_source_system

)

SELECT *
FROM final