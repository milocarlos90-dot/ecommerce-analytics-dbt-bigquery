{% macro singularize(word) %}

{% if word.endswith('ies') %}
    {{ return(word[:-3] ~ 'y') }}
{% elif word.endswith('s') %}
    {{ return(word[:-1]) }}
{% else %}
    {{ return(word) }}
{% endif %}

{% endmacro %}