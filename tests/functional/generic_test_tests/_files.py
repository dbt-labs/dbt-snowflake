SCHEMA__CONTROL = """
version: 2
models:
  - name: colors
    columns:
      - name: color
        data_tests:
          - not_null
"""


SCHEMA__EXPLICIT_WAREHOUSE = """
version: 2
models:
  - name: colors
    columns:
      - name: color
        data_tests:
          - not_null:
              config:
                snowflake_warehouse: DBT_TESTING_ALT
"""


SCHEMA__NOT_NULL = """
version: 2
models:
  - name: facts
    columns:
      - name: value
        data_tests:
          - not_null:
              config:
                snowflake_warehouse: DBT_TESTING_ALT
"""


SCHEMA__RELATIONSHIPS = """
version: 2
models:
  - name: facts
    columns:
      - name: color
        data_tests:
          - relationships:
              to: ref('my_colors')
              field: color
              config:
                snowflake_warehouse: DBT_TESTING_ALT
"""


SCHEMA__ACCEPTED_VALUES = """
version: 2
models:
  - name: colors
    columns:
      - name: color
        data_tests:
          - accepted_values:
              values: ['blue', 'red', 'green']
              config:
                snowflake_warehouse: DBT_TESTING_ALT
"""


SEED__COLORS = """
color
blue
green
red
yellow
""".strip()


# record 10 is missing a value
# record 7 has a color that's not on COLORS
SEED__FACTS = """
id,color,value
1,blue,10
2,red,20
3,green,30
4,yellow,40
5,blue,50
6,red,60
7,orange,70
8,green,80
9,yellow,90
10,blue,
""".strip()
