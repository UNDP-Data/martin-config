SELECT has_function_privilege('{user}', oid::regproc, 'execute') as execute
FROM pg_proc
WHERE proname = '{function_name}' AND pronamespace::regnamespace::text = '{schema}';
