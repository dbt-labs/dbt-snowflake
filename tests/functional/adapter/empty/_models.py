SEED = """
id,value
1,a
2,b
3,c
""".strip()


CONTROL = """
select * from {{ ref("my_seed") }}
"""


GET_COLUMNS_IN_RELATION = """
{{
    config(materialized="table")
}}
select {{ adapter.get_columns_in_relation(ref("my_seed"))|length }} as columns
"""
