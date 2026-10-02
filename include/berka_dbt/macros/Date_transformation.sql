
{% macro yymmdd_to_date(column_name) -%}
    make_date(
        1900 + (cast({{ column_name }} as integer) // 10000),
        (cast({{ column_name }} as integer) // 100) % 100,
        cast({{ column_name }} as integer) % 100
    )
{%- endmacro %}