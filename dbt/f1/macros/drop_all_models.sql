{% macro drop_all_models(dry_run=false) %}
    {% set models = graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("package_name", "equalto", project_name)
        | list %}

    {% for node in models %}
        {% set relation = adapter.get_relation(
            database=node.database,
            schema=node.schema,
            identifier=node.alias or node.name
        ) %}

        {% if relation is not none %}
            {% set drop_type = "view" if relation.type == "view" else "table" %}
            {% set drop_stmt = "drop " ~ drop_type ~ " if exists " ~ relation %}

            {% if dry_run %}
                {{ log(drop_stmt, info=true) }}
            {% else %}
                {{ log("Dropping: " ~ relation ~ " (" ~ drop_type ~ ")", info=true) }}
                {% do run_query(drop_stmt) %}
            {% endif %}
        {% else %}
            {{ log("Skipping (not found): " ~ node.schema ~ "." ~ (node.alias or node.name), info=true) }}
        {% endif %}
    {% endfor %}
{% endmacro %}
