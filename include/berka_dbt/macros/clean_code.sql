{#
    Normalises a coded text column.

    Profiling showed "no value" appears in TWO forms in the source:
        - NULL               (481,881 rows in k_symbol)
        - whitespace ' '     (53,433 rows in k_symbol)
    Both mean the same thing, so both become the single code 'N/A'.

    How it works, inside out:
        trim(x)              ' '  -> ''       (remove spaces)
        nullif(..., '')      ''   -> NULL     (empty text becomes NULL)
        coalesce(..., 'N/A') NULL -> 'N/A'    (NULL becomes the code)
    Real codes are only trimmed, never renamed (traceability to the source).
#}
{% macro clean_code(column_name) -%}
    coalesce(nullif(trim({{ column_name }}), ''), 'N/A')
{%- endmacro %}