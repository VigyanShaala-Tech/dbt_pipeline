{{ config(
  materialized='table'
) }}

WITH cohort_students AS (

    SELECT
        sc.student_code,
        sc.student_id,
        sc.cohort_code,
        sc.is_leader,
        sc.cohort_enroll_date,

        CASE
            WHEN sc.cohort_code LIKE 'INC%'
                 AND CAST(SUBSTRING(sc.cohort_code, 4) AS INTEGER) <= 9
            THEN 'LEGACY_INC'

            WHEN sc.cohort_code LIKE 'INC%'
                 AND CAST(SUBSTRING(sc.cohort_code, 4) AS INTEGER) >= 10
            THEN 'MODERN_INC'

            WHEN sc.cohort_code LIKE 'ACC%'
            THEN 'ACC'
        END AS cohort_type,

        /* ACC004-specific cutoff */
        CASE
            WHEN sc.cohort_code = 'ACC004'
            THEN DATE '2026-02-13'
        END AS cutoff_date

    FROM {{ source('raw', 'student_cohort') }} sc
	--WHERE sc.cohort_code IN (
        --'INC012'
		--'INC012'
    --) 

),

latest_education AS (

    SELECT *
    FROM (

        SELECT
            cs.student_id,
            cs.cohort_code,

            se.id,
            se.education_course_id,
            se.subject_id,
            se.interest_subject_id,
            se.college_id,
            se.university_id,
            se.college_location_id,
            se.start_year,
            se.end_year,
            se.inserted_at,
            se.updated_at,

            ROW_NUMBER() OVER (
                PARTITION BY cs.student_id, cs.cohort_code
                ORDER BY

                    /* Legacy INC: highest ID among NULL timestamp records */
                    CASE
                        WHEN cs.cohort_type = 'LEGACY_INC'
                             AND se.updated_at IS NULL
                             AND se.inserted_at IS NULL
                        THEN se.id
                    END DESC,

                    /* Modern INC + ACC */
                    CASE
                        WHEN cs.cohort_type IN ('MODERN_INC', 'ACC')
                             AND (
                                 cs.cutoff_date IS NULL
                                 OR COALESCE(se.updated_at, se.inserted_at)::date
                                    <= cs.cutoff_date
                             )
                        THEN se.updated_at
                    END DESC NULLS LAST,

                    CASE
                        WHEN cs.cohort_type IN ('MODERN_INC', 'ACC')
                             AND (
                                 cs.cutoff_date IS NULL
                                 OR COALESCE(se.updated_at, se.inserted_at)::date
                                    <= cs.cutoff_date
                             )
                        THEN se.inserted_at
                    END DESC NULLS LAST,

                    /* Final tie breaker */
                    se.id DESC

            ) AS rn

        FROM cohort_students cs

        JOIN {{ source('raw', 'student_education') }} se
            ON cs.student_id = se.student_id

        WHERE
            (
                cs.cohort_type = 'LEGACY_INC'
                AND se.updated_at IS NULL
                AND se.inserted_at IS NULL
            )

            OR

            (
                cs.cohort_type IN ('MODERN_INC', 'ACC')
                AND (
                    cs.cutoff_date IS NULL
                    OR COALESCE(se.updated_at, se.inserted_at)::date
                       <= cs.cutoff_date
                )
            )

    ) ranked_education

    WHERE rn = 1

),

latest_registration AS (

    SELECT *
    FROM (

        SELECT
            cs.student_id,
            cs.cohort_code,

            sr.id,
            sr.form_details,
            sr.assigned_through,

            sr.form_details ->> 'new_college_name'
                AS new_college_name,

            sr.form_details ->> 'new_university_name'
                AS new_university_name,

            sr.form_details ->> 'currently_pursuing_year'
                AS currently_pursuing_year,

            sr.form_details ->> 'partner_organization'
                AS partner_organization,
            
            sr.form_details ->> 'student_category'
                AS student_category,

            sr.registration_date,
            sr.inserted_at,
            sr.updated_at,

            ROW_NUMBER() OVER (
                PARTITION BY cs.student_id, cs.cohort_code
                ORDER BY

                    /* Legacy INC: highest ID among NULL timestamp records */
                    CASE
                        WHEN cs.cohort_type = 'LEGACY_INC'
                             AND sr.updated_at IS NULL
                             AND sr.inserted_at IS NULL
                        THEN sr.id
                    END DESC,

                    /* Modern INC + ACC */
                    CASE
                        WHEN cs.cohort_type IN ('MODERN_INC', 'ACC')
                             AND (
                                 cs.cutoff_date IS NULL
                                 OR COALESCE(sr.updated_at, sr.inserted_at)::date
                                    <= cs.cutoff_date
                             )
                        THEN sr.updated_at
                    END DESC NULLS LAST,

                    CASE
                        WHEN cs.cohort_type IN ('MODERN_INC', 'ACC')
                             AND (
                                 cs.cutoff_date IS NULL
                                 OR COALESCE(sr.updated_at, sr.inserted_at)::date
                                    <= cs.cutoff_date
                             )
                        THEN sr.inserted_at
                    END DESC NULLS LAST,

                    /* Final tie breaker */
                    sr.id DESC

            ) AS rn

        FROM cohort_students cs

        JOIN {{ source('raw', 'student_registration_details') }} sr
            ON cs.student_id = sr.student_id

        WHERE
            (
                cs.cohort_type = 'LEGACY_INC'
                AND sr.updated_at IS NULL
                AND sr.inserted_at IS NULL
            )

            OR

            (
                cs.cohort_type IN ('MODERN_INC', 'ACC')
                AND (
                    cs.cutoff_date IS NULL
                    OR COALESCE(sr.updated_at, sr.inserted_at)::date
                       <= cs.cutoff_date
                )
            )

    ) ranked_registration

    WHERE rn = 1

),
subject_mappings AS (

    SELECT
        se.student_id,
        se.cohort_code,
        se.education_course_id,

        sm.education_category,
        sm.subject_area,
        sm.sub_field

    FROM latest_education se

    JOIN LATERAL unnest(
        COALESCE(se.subject_id, ARRAY[]::integer[])
    ) AS subj(subject_id)
        ON TRUE

    JOIN {{ source('raw', 'subject_mapping') }} sm
        ON subj.subject_id = sm.id
),

aggregated_subjects AS (

    SELECT
        student_id,
        cohort_code,
        education_course_id,

        STRING_AGG(
            DISTINCT education_category,
            ', '
            ORDER BY education_category
        ) AS education_categories,

        STRING_AGG(
            DISTINCT subject_area,
            ', '
            ORDER BY subject_area
        ) AS subject_areas,

        STRING_AGG(
            DISTINCT sub_field,
            ', '
            ORDER BY sub_field
        ) AS sub_fields_list

    FROM subject_mappings

    GROUP BY
        student_id,
        cohort_code,
        education_course_id
),


interest_subject_mappings AS (

    SELECT
        se.student_id,
        se.cohort_code,
        se.education_course_id,

        sm.education_category,
        sm.subject_area,
        sm.sub_field

    FROM latest_education se

    JOIN LATERAL unnest(
        COALESCE(se.interest_subject_id, ARRAY[]::integer[])
    ) AS subj(subject_id)
        ON TRUE

    JOIN {{ source('raw', 'subject_mapping') }} sm
        ON subj.subject_id = sm.id
),

aggregated_interest_subjects AS (

    SELECT
        student_id,
        cohort_code,
        education_course_id,

        STRING_AGG(
            DISTINCT education_category,
            ', '
            ORDER BY education_category
        ) AS interest_education_categories,

        STRING_AGG(
            DISTINCT subject_area,
            ', '
            ORDER BY subject_area
        ) AS interest_subject_areas,

        STRING_AGG(
            DISTINCT sub_field,
            ', '
            ORDER BY sub_field
        ) AS interest_sub_fields_list

    FROM interest_subject_mappings

    GROUP BY
        student_id,
        cohort_code,
        education_course_id
)


SELECT
    cs.student_code,
    cs.student_id,
    cs.cohort_code,
    cs.is_leader,
    cs.cohort_enroll_date,

    /* Student Details */
    sd.id AS student_detail_id,
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

    /* Education */
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

    /* Current Subject mapping */
    asub.education_categories,
    asub.subject_areas,
    asub.sub_fields_list,

	/* Interest subject mapping */
	aisub.interest_education_categories,
	aisub.interest_subject_areas,
	aisub.interest_sub_fields_list,

    crm.course_name,
    cm.standard_college_names AS college_name,
    um.standard_university_names AS university_name,

    /* Registration-derived mappings */
    sr.new_college_name,
    sr.new_university_name,

    CASE
        WHEN cm.standard_college_names IS NULL
            OR TRIM(cm.standard_college_names) = ''
        THEN sr.new_college_name
        ELSE cm.standard_college_names
    END AS final_college_name,

    CASE
        WHEN um.standard_university_names IS NULL
            OR TRIM(um.standard_university_names) = ''
        THEN sr.new_university_name
        ELSE um.standard_university_names
    END AS final_university_name,

    /* Registration */
    sr.id AS registration_id,
    sr.registration_date,
    sr.assigned_through,
    sr.currently_pursuing_year,
    sr.partner_organization,
    sr.student_category,
    sr.form_details,
    sr.inserted_at AS registration_inserted_at,
    sr.updated_at AS registration_updated_at,

    /* Location mapping */
    lm.state_union_territory,
    lm.district,
    lm.country,
    lm.city_category

FROM cohort_students cs

INNER JOIN {{ source('raw', 'student_details') }} sd
    ON cs.student_id = sd.id

LEFT JOIN latest_education se
    ON cs.student_id = se.student_id
   AND cs.cohort_code = se.cohort_code

LEFT JOIN {{ source('raw', 'college_mapping') }} cm
    ON se.college_id = cm.college_id

LEFT JOIN {{ source('raw', 'course_mapping') }} crm
    ON se.education_course_id = crm.course_id

LEFT JOIN {{ source('raw', 'university_mapping') }} um
    ON se.university_id = um.university_id

LEFT JOIN latest_registration sr
    ON cs.student_id = sr.student_id
   AND cs.cohort_code = sr.cohort_code

LEFT JOIN {{ source('raw', 'location_mapping') }} lm
    ON sd.location_id = lm.location_id

LEFT JOIN aggregated_subjects asub
    ON cs.student_id = asub.student_id
   AND cs.cohort_code = asub.cohort_code
   AND se.education_course_id = asub.education_course_id

LEFT JOIN aggregated_interest_subjects aisub
    ON cs.student_id = aisub.student_id
   AND cs.cohort_code = aisub.cohort_code
   AND se.education_course_id = aisub.education_course_id