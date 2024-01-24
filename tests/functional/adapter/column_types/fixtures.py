_MODEL_SQL = """
select
    1::smallint as smallint_col,
    2::int as int_col,
    3::bigint as bigint_col,
    4::integer as integer_col,
    5::tinyint as tinyint_col,
    6::byteint as byteint_col,
    7.0::float as float_col,
    8.0::float4 as float4_col,
    9.0::float8 as float8_col,
    10.0::double as double_col,
    11.0::double precision as double_p_col,
    12.0::real as real_col,
    13.0::numeric as numeric_col,
    14.0::decimal as decimal_col,
    15.0::number as number_col,
    '16'::text as text_col,
    '17'::varchar(20) as varchar_col
"""

_SCHEMA_YML = """
version: 2
models:
  - name: model
    data_tests:
      - is_type:
          column_map:
            smallint_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            int_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            bigint_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            integer_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            tinyint_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            byteint_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            float_col: ['float', 'number', 'not string', 'not integer', 'not numeric']
            float4_col: ['float', 'number', 'not string', 'not integer', 'not numeric']
            float8_col: ['float', 'number', 'not string', 'not integer', 'not numeric']
            double_col: ['float', 'number', 'not string', 'not integer', 'not numeric']
            double_p_col: ['float', 'number', 'not string', 'not integer', 'not numeric']
            real_col: ['float', 'number', 'not string', 'not integer', 'not numeric']
            numeric_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            decimal_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            number_col: ['numeric', 'number', 'not string', 'not float', 'not integer']
            text_col: ['string', 'not number']
            varchar_col: ['string', 'not number']
"""
