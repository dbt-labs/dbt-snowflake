FACT = "select 1 as my_column"


INVOKE_IS_RELATION = """
select
    '{{ get_relation() }}'                 as get_relation,
    {{ check_get_relation_is_relation() }} as is_relation
"""


# Purposely pointing out that the models are the same, except for the call to `ref()`
INVOKE_IS_RELATION_WITH_REF = INVOKE_IS_RELATION + "\nfrom {{ ref('FACT') }}"
