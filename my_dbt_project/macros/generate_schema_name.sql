{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ defaultschema }}

    {%- else -%}
        {%- if target.schema != "production" -%}
            {{target.schema}}_{{ custom_schema_name }}
        {%- else -%}
         {{custom_schema_name}}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}