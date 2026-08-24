{{
  config(
    materialized='table',
    tags=['staging', 'student_education']
  )
}}

with osot_rows as (

    select
        se.id::text as id,
        se.student_id::text as student_id,

        se.education_course_id,
        se.subject_id,
        se.interest_subject_id,
        se.college_id,
        se.university_id,
        se.college_location_id,

        se.start_year,
        se.end_year,

        -- education_course_id -> course_mapping.course_id
        crm.course_name::text as course_name_raw,

        -- subject_id[] -> subject_mapping
        subject_values.education_category_raw,
        subject_values.subject_area_raw,
        subject_values.sub_field_raw,

        -- interest_subject_id[] -> subject_mapping
        interest_subject_values.interest_education_category_raw,
        interest_subject_values.interest_subject_area_raw,
        interest_subject_values.interest_sub_field_raw,

        -- college_id -> college_mapping.college_id
        cm.standard_college_names::text as college_name_raw,

        -- university_id -> university_mapping.university_id
        um.standard_university_names::text as university_name_raw,

        -- college_location_id -> location_mapping.location_id
        lm.country::text as country_raw,
        lm.state_union_territory::text as state_union_territory_raw,
        lm.district::text as district_raw,
        lm.city_category::text as city_category_raw,

        se.inserted_at,
        se.updated_at,

        'OSOT'::text as source_system

    from {{ source('raw', 'student_education') }} se

    -- Map education course
    left join {{ source('raw', 'course_mapping') }} crm
        on se.education_course_id = crm.course_id

    -- Map college
    left join {{ source('raw', 'college_mapping') }} cm
        on se.college_id = cm.college_id

    -- Map university
    left join {{ source('raw', 'university_mapping') }} um
        on se.university_id = um.university_id

    -- Map college location
    left join {{ source('raw', 'location_mapping') }} lm
        on se.college_location_id = lm.location_id

    -- Map current subject IDs
    left join lateral (

        select
            string_agg(
                distinct sm.education_category,
                ', ' order by sm.education_category
            )::text as education_category_raw,

            string_agg(
                distinct sm.subject_area,
                ', ' order by sm.subject_area
            )::text as subject_area_raw,

            string_agg(
                distinct sm.sub_field,
                ', ' order by sm.sub_field
            )::text as sub_field_raw

        from {{ source('raw', 'subject_mapping') }} sm

        where sm.id = any(
            coalesce(se.subject_id, array[]::integer[])
        )

    ) subject_values on true

    -- Map interest subject IDs
    left join lateral (

        select
            string_agg(
                distinct sm.education_category,
                ', ' order by sm.education_category
            )::text as interest_education_category_raw,

            string_agg(
                distinct sm.subject_area,
                ', ' order by sm.subject_area
            )::text as interest_subject_area_raw,

            string_agg(
                distinct sm.sub_field,
                ', ' order by sm.sub_field
            )::text as interest_sub_field_raw

        from {{ source('raw', 'subject_mapping') }} sm

        where sm.id = any(
            coalesce(se.interest_subject_id, array[]::integer[])
        )

    ) interest_subject_values on true

),

openedx_rows as (

    select
        concat('OPENEDX_', cfs.id::text) as id,
        x.student_id::text as student_id,

        cast(null as integer) as education_course_id,
        cast(null as integer[]) as subject_id,
        cast(null as integer[]) as interest_subject_id,
        cast(null as integer) as college_id,
        cast(null as integer) as university_id,
        cast(null as integer) as college_location_id,

        cast(null as integer) as start_year,
        cast(null as integer) as end_year,

        -- Current degree
        cfs.response_data::jsonb ->> 'current_degree_level'
            as course_name_raw,

        cast(null as text)
            as education_category_raw,

        cfs.response_data::jsonb ->> 'currently_pursuing_subject_areas'
            as subject_area_raw,

        cast(null as text)
            as sub_field_raw,

        -- Future / interest subjects
        cast(null as text)
            as interest_education_category_raw,

        cfs.response_data::jsonb ->> 'future_subject_area'
            as interest_subject_area_raw,

        cfs.response_data::jsonb ->> 'future_subject_sub_field'
            as interest_sub_field_raw,

        -- Institution
        cfs.response_data::jsonb ->> 'college_name'
            as college_name_raw,

        cfs.response_data::jsonb ->> 'university_name'
            as university_name_raw,

        -- College location
        cfs.response_data::jsonb ->> 'college_country'
            as country_raw,

        cfs.response_data::jsonb ->> 'college_state_union_territory'
            as state_union_territory_raw,

        cfs.response_data::jsonb ->> 'college_district'
            as district_raw,

        cast(null as text)
            as city_category_raw,

        cfs.created as inserted_at,
        cfs.modified as updated_at,

        'Open edX'::text as source_system

    from {{ source(
        'raw',
        'openedx_cohort_management_form_cohortformsubmission'
    ) }} cfs

    inner join {{ ref('stg_student_id_xref') }} x
        on x.openedx_user_id = cfs.user_id

)

select * from osot_rows
union all
select * from openedx_rows