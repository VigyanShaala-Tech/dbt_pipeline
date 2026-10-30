{{
  config(
    materialized='table',
    tags=['staging', 'resource']
  )
}}

-- Real PK confirmed from final_assignment.sql/final_quiz.sql: resource.id.
with osot_rows as (

    select
        id::text                    as id,
        category::text              as category,
        title::text                 as title,
        description::text           as description,
        location::text              as location,
        resource_link::text         as resource_link,
        is_video_resource::boolean  as is_video_resource,
        total_duration::integer     as total_duration,
        'OSOT'::text                as source_system

    from {{ source('raw', 'resource') }}

),

-- Open edX content catalog. If the catalog can hold more than one row per
-- usage_key (it carries created/modified), keep the latest; harmless if
-- usage_key is already unique.
openedx_catalog as (

    select
        cb.*,
        row_number() over (
            partition by cb.usage_key
            order by cb.modified desc
        ) as rn

    from {{ source('raw', 'Openedx_course_content_catalog_coursecontentblock') }} cb

),

openedx_mapped as (

    select
        usage_key::text             as id,   -- same value the fact tables carry as resource_id
        subsection_name::text       as title,
        section_name::text          as description,
        block_type::text            as category       -- raw block_type, no renaming (problem stays problem)

    from openedx_catalog
    where rn = 1

),

openedx_rows as (

    select
        id,
        category,
        title,
        description,
        null::text                  as location,          -- no source in the catalog table
        null::text                  as resource_link,     -- constructed (domain + course + resource), not sourced
        (category = 'video')        as is_video_resource,
        null::integer               as total_duration,    -- no source in the catalog table (see note)
        'Open edX'::text            as source_system

    from openedx_mapped

)

select * from osot_rows
union all
select * from openedx_rows