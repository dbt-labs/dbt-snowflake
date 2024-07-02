SEED = """
my_id,my_value
1,a
2,b
3,c
""".strip()


CONTROL = """
select * from {{ ref("my_seed") }}
"""


GET_COLUMNS_IN_RELATION = """
{{ config(materialized="table") }}
select {{ adapter.get_columns_in_relation(ref("my_seed"))|length }} as get_columns_in_relation
"""


ALTER_COLUMN_TYPE = """
{{ config(materialized="table") }}
{{ alter_column_type(ref("my_seed"), "MY_VALUE", "string") }}
select * from {{ ref("my_seed") }}
"""
