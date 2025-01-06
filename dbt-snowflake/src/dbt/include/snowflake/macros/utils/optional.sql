{% macro optional(name, value, quote_char = '') %}
{#-
--  Insert optional DDL parameters only when their value is provided; makes DDL statements more readable
--
--  Args:
--  - name: the name of the DDL option
--  - value: the value of the DDL option, may be None
--  - quote_char: the quote character to use (e.g. string), leave blank if unnecessary (e.g. integer or bool)
--  Returns:
--      If the value is not None (e.g. provided by the user), return the option setting DDL
--      If the value is None, return an empty string
-#}
{% if value is not none %}{{ name }} = {{ quote_char }}{{ value }}{{ quote_char }}{% endif %}
{% endmacro %}
