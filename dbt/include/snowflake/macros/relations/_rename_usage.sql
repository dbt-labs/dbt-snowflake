/*
This file was created for the sake of reviewing the two implementation options. This will be deleted prior to merging the PR.

These are examples of the current usage.
*/
------------------------------------------ dbt-adapters ------------------------------------------
{%- macro get_create_backup_sql(relation) -%}
    {{- log('Applying CREATE BACKUP to: ' ~ relation) -}}
    {{- adapter.dispatch('get_create_backup_sql', 'dbt')(relation) -}}
{%- endmacro -%}


{%- macro default__get_create_backup_sql(relation) -%}

    -- get the standard backup name
    {% set backup_relation = make_backup_relation(relation, relation.type) %}

    -- drop any pre-existing backup
    {{ get_drop_sql(backup_relation) }};

    {{ get_rename_sql(relation, backup_relation.identifier) }}

{%- endmacro -%}


{%- macro get_rename_intermediate_sql(relation) -%}
    {{- log('Applying RENAME INTERMEDIATE to: ' ~ relation) -}}
    {{- adapter.dispatch('get_rename_intermediate_sql', 'dbt')(relation) -}}
{%- endmacro -%}


{%- macro default__get_rename_intermediate_sql(relation) -%}

    -- get the standard intermediate name
    {% set intermediate_relation = make_intermediate_relation(relation) %}

    {{ get_rename_sql(intermediate_relation, relation.identifier) }}

{%- endmacro -%}
