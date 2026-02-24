{% macro generate_staging_select(source_name, table_name) %}

{% set relation = source(source_name, table_name) %}
{% set columns = adapter.get_columns_in_relation(relation) %}

{% for column in columns %}

    {% if column.name | lower == 'id' %}
        cast({{ column.name }} as {{ column.data_type | upper }})
            as {{ singularize(table_name) }}_id
    {% else %}
        cast({{ column.name }} as {{ column.data_type | upper }})
            as {{ column.name }}
    {% endif %}

    {% if not loop.last %},{% endif %}

{% endfor %}

{% endmacro %}