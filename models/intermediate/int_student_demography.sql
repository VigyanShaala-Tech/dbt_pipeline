{{
    config(
        materialized='table',
        tags=['intermediate', 'student_demography']
    )
}}

with cohort_students as (

    select
        student_code::text as student_code,
        student_id::text as student_id,
        cohort_code::text as cohort_code,
        is_leader,
        cohort_enroll_date,

        case
            when cohort_code like 'INC%'
                 and substring(cohort_code from 4)::integer <= 9
            then 'LEGACY_INC'

            when cohort_code like 'INC%'
                 and substring(cohort_code from 4)::integer >= 10
            then 'MODERN_INC'

            when cohort_code like 'ACC%'
            then 'ACC'

            else 'OTHER'
        end as cohort_type,

        case
            when cohort_code = 'ACC004'
            then date '2026-02-13'
            else null
        end as cutoff_date

    from {{ ref('stg_student_cohort') }}

),

student_details as (

    /*
      The staging model preserves OSOT and Open edX rows.

      OSOT student_id:
          historical OSOT ID

      Open edX matched student:
          same OSOT student_id

      Open edX unmatched student:
          openedx_<user_id>

      We retain one student-details record per student here.
      Prefer OSOT when both OSOT and Open edX exist for the same student_id.
    */

    select *
    from (

        select
            sd.*,

            row_number() over (
                partition by sd.student_id
                order by
                    case
                        when sd.source_system = 'OSOT' then 1
                        else 2
                    end,
                    sd.updated_at desc nulls last,
                    sd.inserted_at desc nulls last
            ) as rn

        from {{ ref('stg_student_details') }} sd

    ) ranked

    where rn = 1

),

latest_education as (

    select *
    from (

        select
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

            row_number() over (

                partition by
                    cs.student_id,
                    cs.cohort_code

                order by

                    /*
                      Legacy INC:
                      retain the original historical selection logic.
                    */
                    case
                        when cs.cohort_type = 'LEGACY_INC'
                             and se.updated_at is null
                             and se.inserted_at is null
                        then se.id
                    end desc nulls last,

                    /*
                      Modern INC and ACC:
                      select the latest education record before
                      the cohort cutoff date.
                    */
                    case
                        when cs.cohort_type in ('MODERN_INC', 'ACC')
                             and (
                                 cs.cutoff_date is null
                                 or coalesce(
                                     se.updated_at,
                                     se.inserted_at
                                 )::date <= cs.cutoff_date
                             )
                        then se.updated_at
                    end desc nulls last,

                    case
                        when cs.cohort_type in ('MODERN_INC', 'ACC')
                             and (
                                 cs.cutoff_date is null
                                 or coalesce(
                                     se.updated_at,
                                     se.inserted_at
                                 )::date <= cs.cutoff_date
                             )
                        then se.inserted_at
                    end desc nulls last,

                    /*
                      Prefer the OSOT row if two otherwise equivalent
                      records exist for a matched student.
                    */
                    case
                        when se.source_system = 'OSOT' then 1
                        else 2
                    end,

                    se.id desc nulls last

            ) as rn

        from cohort_students cs

        inner join {{ ref('stg_student_education') }} se
            on cs.student_id = se.student_id

        where

            (
                cs.cohort_type = 'LEGACY_INC'
                and se.updated_at is null
                and se.inserted_at is null
            )

            or

            (
                cs.cohort_type in ('MODERN_INC', 'ACC')
                and (
                    cs.cutoff_date is null
                    or coalesce(
                        se.updated_at,
                        se.inserted_at
                    )::date <= cs.cutoff_date
                )
            )

            or

            /*
              For Open edX / OTHER cohorts where the historical
              INC/ACC rules do not apply.
            */
            (
                cs.cohort_type = 'OTHER'
            )

    ) ranked_education

    where rn = 1

),

latest_registration as (

    select *
    from (

        select
            cs.student_id,
            cs.cohort_code,

            sr.registration_id,
            sr.assigned_through,
            sr.form_details,

            sr.registration_date,
            sr.inserted_at,
            sr.updated_at,

            sr.source_system,

            row_number() over (

                partition by
                    cs.student_id,
                    cs.cohort_code

                order by

                    /*
                      Legacy INC historical logic.
                    */
                    case
                        when cs.cohort_type = 'LEGACY_INC'
                             and sr.updated_at is null
                             and sr.inserted_at is null
                        then sr.registration_id
                    end desc nulls last,

                    /*
                      Modern INC + ACC:
                      latest record before cutoff.
                    */
                    case
                        when cs.cohort_type in ('MODERN_INC', 'ACC')
                             and (
                                 cs.cutoff_date is null
                                 or coalesce(
                                     sr.updated_at,
                                     sr.inserted_at
                                 )::date <= cs.cutoff_date
                             )
                        then sr.updated_at
                    end desc nulls last,

                    case
                        when cs.cohort_type in ('MODERN_INC', 'ACC')
                             and (
                                 cs.cutoff_date is null
                                 or coalesce(
                                     sr.updated_at,
                                     sr.inserted_at
                                 )::date <= cs.cutoff_date
                             )
                        then sr.inserted_at
                    end desc nulls last,

                    /*
                      OSOT preference for equivalent matched rows.
                    */
                    case
                        when sr.source_system = 'OSOT' then 1
                        else 2
                    end,

                    sr.registration_id desc nulls last

            ) as rn

        from cohort_students cs

        inner join {{ ref('stg_student_registration_details') }} sr
            on cs.student_id = sr.student_id

        where

            (
                cs.cohort_type = 'LEGACY_INC'
                and sr.updated_at is null
                and sr.inserted_at is null
            )

            or

            (
                cs.cohort_type in ('MODERN_INC', 'ACC')
                and (
                    cs.cutoff_date is null
                    or coalesce(
                        sr.updated_at,
                        sr.inserted_at
                    )::date <= cs.cutoff_date
                )
            )

            or

            (
                cs.cohort_type = 'OTHER'
            )

    ) ranked_registration

    where rn = 1

),

final as (

    select

        /* =====================================================
           STUDENT + COHORT
           ===================================================== */

        cs.student_code,
        cs.student_id,
        cs.cohort_code,
        cs.is_leader,
        cs.cohort_enroll_date,


        /* =====================================================
           STUDENT DETAILS
           ===================================================== */

        sd.student_id as student_detail_id,

        sd.email,
        sd.first_name,
        sd.last_name,
        sd.gender,
        sd.phone,

        sd.date_of_birth,
        sd.caste,
        sd.annual_family_income_inr,

        sd.location_id,

        sd.inserted_at as student_inserted_at,
        sd.updated_at as student_updated_at,

        sd.source_system as student_source_system,


        /* =====================================================
           EDUCATION
           ===================================================== */

        se.id as education_id,

        se.education_course_id,
        se.subject_id,
        se.interest_subject_id,

        se.college_id,
        se.university_id,
        se.college_location_id,

        se.start_year,
        se.end_year,

        se.inserted_at as education_inserted_at,
        se.updated_at as education_updated_at,

        se.source_system as education_source_system,


        /* =====================================================
           COURSE
           OSOT: mapped from course_mapping
           Open edX: current_degree_level
           ===================================================== */

        se.course_name_raw as course_name,


        /* =====================================================
           CURRENT SUBJECT
           OSOT: subject_mapping aggregation
           Open edX: currently_pursuing_subject_areas
           ===================================================== */

        se.education_category_raw
            as education_categories,

        se.subject_area_raw
            as subject_areas,

        se.sub_field_raw
            as sub_fields_list,


        /* =====================================================
           INTEREST SUBJECT
           ===================================================== */

        se.interest_education_category_raw
            as interest_education_categories,

        se.interest_subject_area_raw
            as interest_subject_areas,

        se.interest_sub_field_raw
            as interest_sub_fields_list,


        /* =====================================================
           COLLEGE / UNIVERSITY
           ===================================================== */

        se.college_name_raw
            as college_name,

        se.university_name_raw
            as university_name,


        /*
          These names are retained separately because they may
          originate from registration/form data.
        */

        sr.form_details::jsonb ->> 'new_college_name'
            as new_college_name,

        sr.form_details::jsonb ->> 'new_university_name'
            as new_university_name,


        /*
          Priority:

          1. Education staging value
          2. Registration form fallback
        */

        coalesce(
            nullif(trim(se.college_name_raw), ''),
            nullif(
                trim(sr.form_details::jsonb ->> 'new_college_name'),
                ''
            )
        ) as final_college_name,

        coalesce(
            nullif(trim(se.university_name_raw), ''),
            nullif(
                trim(sr.form_details::jsonb ->> 'new_university_name'),
                ''
            )
        ) as final_university_name,


        /* =====================================================
           REGISTRATION
           ===================================================== */

        sr.registration_id,

        sr.registration_date,
        sr.assigned_through,

        /*
          These keys exist for historical OSOT registration JSON.
          For Open edX they will return NULL unless present.
        */

        sr.form_details::jsonb ->> 'currently_pursuing_year'
            as currently_pursuing_year,

        sr.form_details::jsonb ->> 'partner_organization'
            as partner_organization,

        sr.form_details::jsonb ->> 'student_category'
            as student_category,

        sr.form_details,

        sr.inserted_at as registration_inserted_at,
        sr.updated_at as registration_updated_at,

        sr.source_system as registration_source_system,


        /* =====================================================
           LOCATION

           The staging education/student models already resolve
           OSOT mapping and preserve Open edX raw geography.
           ===================================================== */

        coalesce(
            se.country_raw,
            sd.country_raw
        ) as country,

        coalesce(
            se.state_union_territory_raw,
            sd.state_union_territory_raw
        ) as state_union_territory,

        coalesce(
            se.district_raw,
            sd.district_raw
        ) as district,

        coalesce(
            se.city_category_raw,
            sd.city_category_raw
        ) as city_category

    from cohort_students cs

    inner join student_details sd
        on cs.student_id = sd.student_id

    left join latest_education se
        on cs.student_id = se.student_id
       and cs.cohort_code = se.cohort_code

    left join latest_registration sr
        on cs.student_id = sr.student_id
       and cs.cohort_code = sr.cohort_code

)

select *
from final