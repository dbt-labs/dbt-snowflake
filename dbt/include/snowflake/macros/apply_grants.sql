{% macro snowflake__copy_grants() %}
    {% set copy_grants = config.get('copy_grants', False) %}
    {{ return(copy_grants) }}
{% endmacro %}

{%- macro snowflake__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}

{#
  -- Create versions of get_grant_sql and get_revoke_sql that support an additional
  -- object_type parameter
#}

{% macro get_grant_sql_by_type(relation, privilege, object_type, grantees) %}
    {{ return(adapter.dispatch('get_grant_sql_by_type', 'dbt-snowflake')(relation, privilege, object_type, grantees)) }}
{% endmacro %}

{%- macro snowflake__get_grant_sql_by_type(relation, privilege, object_type, grantees) -%}
    grant {{ privilege }} on {{ relation.render() }} to {{object_type | replace('_', ' ')}} {{ grantees | join(', ') }}
{%- endmacro -%}

{% macro get_revoke_sql_by_type(relation, privilege, object_type, grantees) %}
    {{ return(adapter.dispatch('get_revoke_sql_by_type', 'dbt-snowflake')(relation, privilege, object_type, grantees)) }}
{% endmacro %}

{%- macro snowflake__get_revoke_sql_by_type(relation, privilege, object_type, grantees) -%}
    revoke {{ privilege }} on {{ relation.render() }} from {{object_type | replace('_', ' ')}} {{ grantees | join(', ') }}
{%- endmacro -%}


{% macro get_dcl_statement_list_by_type(relation, grant_config_by_type, get_dcl_macro) %}
    {{ return(adapter.dispatch('get_dcl_statement_list_by_type', 'dbt-snowflake')(relation, grant_config_by_type, get_dcl_macro)) }}
{% endmacro %}


{%- macro snowflake__get_dcl_statement_list_by_type(relation, grant_config_by_type, get_dcl_macro) -%}
    {#
      -- Unpack grant_config into specific privileges and the set of users who need them granted/revoked.
      -- Depending on whether this database supports multiple grantees per statement, pass in the list of
      -- all grantees per privilege, or (if not) template one statement per privilege-grantee pair.
      -- `get_dcl_macro` will be either `get_grant_sql_by_type` or `get_revoke_sql_by_type`
      --
      -- grant_config_by_type should be in the following format  { grantee_type: { privilege: [grantee] } }
    #}
    {%- set dcl_statements = [] -%}
    {%- for object_type, config in grant_config_by_type.items() %}
        {%- for privilege, grantees in config.items() %}
            {%- if support_multiple_grantees_per_dcl_statement() and grantees -%}
            {%- set dcl = get_dcl_macro(relation, privilege, object_type, grantees) -%}
            {%- do dcl_statements.append(dcl) -%}
            {%- else -%}
            {%- for grantee in grantees -%}
                {% set dcl = get_dcl_macro(relation, privilege, object_type, [grantee]) %}
                {%- do dcl_statements.append(dcl) -%}
            {% endfor -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
    {{ return(dcl_statements) }}
{%- endmacro %}

{% macro split_grants_by_grantee_type(grant_config) %}
    {#
      -- Takes grant_config in { privilege: [grantee] } or { privilege: { grantee_type: [ grantee ] } }
      -- and converts to { grantee_type: { privilege: [grantee] } }
      --
      -- Assumes { privilege: [grantee] } maps to { privilege: {'role': [ grantee ] } }
    #}
    {% set converted_dict = {} %}
    {% for grant_config_privilege, privilege_collection in grant_config.items() %}
        {#-- loop through the role entries and handle mapping, list & string entries --#}
        {% for privilege_item in privilege_collection %}
            {#-- Assume old style list grants map to role --#}
            {% if privilege_item is not mapping %}
                {% set privilege_item = {"role": privilege_item} %}
            {% endif %}

            {% for grantee_type, grantees in privilege_item.items() %}
                {#-- Make sure object_type is in grant_config_by_type --#}
                {% set grantee_type_privileges = converted_dict.setdefault(grantee_type|lower, {}) %}
                {% set privilege_list = grantee_type_privileges.setdefault(grant_config_privilege, []) %}

                {#-- convert string to array to make code simpler --#}
                {% if grantees is string %}
                    {% set grantees = [grantees] %}
                {% endif %}

                {% for grantee in grantees %}
                    {#-- Only add the item if not already in the list --#}
                    {% if grantee not in privilege_list %}
                        {% set _ = privilege_list.append(grantee) %}
                    {% endif %}
                {% endfor %}
            {% endfor %}

        {% endfor %}
    {% endfor %}
    {{ return(converted_dict) }}
{% endmacro %}

{% macro snowflake__apply_grants(relation, grant_config, should_revoke=True) %}
    {#-- If grant_config is {} or None, this is a no-op --#}
    {% if grant_config %}

        {{ log('grant_config: ' ~ grant_config) }}
        {#-- Check if we have defined new role type or are using default style --#}
        {% set desired_grants_dict =  adapter.standardize_grant_config(grant_config) %}
        {{ log('desired_grants_dict: ' ~ desired_grants_dict) }}


        {% if should_revoke %}
            {#-- We think previous grants may have carried over --#}
            {#-- Show current grants and calculate diffs --#}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}

            {% set needs_granting = adapter.diff_of_grants(desired_grants_dict, current_grants_dict) %}
            {% set needs_revoking = adapter.diff_of_grants(current_grants_dict, desired_grants_dict) %}

            {#-- TODO: remove debug log statement --#}
            {{log ('needs_granting : ' ~ needs_granting)}}
            {{log ('needs_revoking : ' ~ needs_revoking)}}

            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation.render() ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {#-- We don't think there's any chance of previous grants having carried over. --#}
            {#-- Jump straight to granting what the user has configured. --#}
            {% set needs_revoking = {} %}
            {% set needs_granting = desired_grants_dict %}
        {% endif %}

        {% if needs_granting or needs_revoking %}
            {% set revoke_statement_list = get_dcl_statement_list_by_type(relation, needs_revoking, snowflake__get_revoke_sql_by_type) %}
            {% set grant_statement_list = get_dcl_statement_list_by_type(relation, needs_granting, snowflake__get_grant_sql_by_type) %}
            {% set dcl_statement_list = revoke_statement_list + grant_statement_list %}

            {#-- TODO: remove debug log statement --#}
            {{ log('dcl_statement_list: ' ~ dcl_statement_list) }}
            {% if dcl_statement_list %}
                {{ call_dcl_statements(dcl_statement_list) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
