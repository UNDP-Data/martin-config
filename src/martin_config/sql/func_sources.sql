SELECT
    n.nspname AS function_schema,
    p.proname AS function_name,
	array_to_string(p.proargnames, '') AS func_args
FROM
    pg_proc p
    LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE
    n.nspname NOT IN ('pg_catalog', 'information_schema') AND
	array_to_string(p.proargnames, '') LIKE 'zxyquery_params'

ORDER BY
    function_schema,
    function_name;