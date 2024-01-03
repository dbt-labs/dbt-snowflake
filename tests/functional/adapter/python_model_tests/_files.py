# <temporary>_<transient>_table
TRANSIENT_TRUE_TABLE = """
import pandas

def model(dbt, session):
    dbt.config(transient=True)
    return pandas.DataFrame([[1,2]] * 10, columns=['test', 'test2'])
"""


TRANSIENT_FALSE_TABLE = """
import pandas

def model(dbt, session):
    dbt.config(transient=False)
    return pandas.DataFrame([[1,2]] * 10, columns=['test', 'test2'])
"""


TRANSIENT_NONE_TABLE = """
import pandas

def model(dbt, session):
    dbt.config(transient=None)
    return pandas.DataFrame([[1,2]] * 10, columns=['test', 'test2'])
"""


TRANSIENT_UNSET_TABLE = """
import pandas

def model(dbt, session):
    return pandas.DataFrame([[1,2]] * 10, columns=['test', 'test2'])
"""


MACRO__DESCRIBE_TABLES = """
{% macro snowflake__test__describe_tables() %}
    {%- set _sql -%}
        show tables;
        select "name", "kind"
        from table(result_scan(last_query_id()))
    {%- endset %}
    {% set _table = run_query(_sql) %}

    {% do return(_table) %}
{% endmacro %}
"""
