/*
  generate_schema_name override
  ==============================
  Default dbt behaviour: final_schema = {target.schema}_{custom_schema}
  This causes doubling (e.g. gold_gold) when profiles.yml already sets schema=gold.

  This override: if a custom schema is explicitly set (via +schema in dbt_project.yml),
  use it as-is. Otherwise fall back to target.schema from the active profile/connection.

  This means dbt_project.yml is the single source of truth for schema names,
  which works correctly both locally (profiles.yml) and in CI/CD or Databricks Workflows
  (where profiles.yml is not present and connection is injected by the job task config).
*/
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
