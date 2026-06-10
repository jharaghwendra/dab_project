/*
  Custom generic test: accepted_values_case_insensitive
  ======================================================
  Purpose:
    Like the built-in `accepted_values` test, but performs a case-insensitive
    comparison. Useful for columns that may arrive in mixed case from upstream
    (e.g. 'Deposit', 'DEPOSIT', 'deposit' should all be valid).

  Arguments:
    model        — injected automatically by dbt (the model being tested)
    column_name  — injected automatically by dbt (the column being tested)
    values       — list of allowed values (case-insensitive match)

  Usage in yml:
    columns:
      - name: payment_type
        tests:
          - accepted_values_case_insensitive:
              values: ["DEPOSIT", "WITHDRAWAL", "REFUND"]

  Returns rows that FAIL (violate the rule).
  0 rows = PASS. Any rows = FAIL.
*/

{% test accepted_values_case_insensitive(model, column_name, values) %}

    select
        {{ column_name }} as failing_value,
        count(*)          as row_count
    from {{ model }}
    where upper({{ column_name }}) not in (
        {% for val in values %}
            upper('{{ val }}')
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    )
    group by {{ column_name }}

{% endtest %}
