{# 
    This macro returns the desciption of the payment type 
#}

{% macro get_payment_type_description(payment_type) -%}

    CASE {{ payment_type }}
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
    END

{% endmacro %}