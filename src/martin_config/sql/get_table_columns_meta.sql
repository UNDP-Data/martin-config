SELECT
    pg_attribute.attname AS column_name,
    pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type,
    tp.typname AS type_name
FROM 
    pg_catalog.pg_attribute
INNER JOIN 
    pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid
INNER JOIN 
    pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
INNER JOIN 
    pg_catalog.pg_type AS tp ON tp.oid = pg_attribute.atttypid
WHERE
    pg_attribute.attnum > 0
    AND NOT pg_attribute.attisdropped
    AND pg_namespace.nspname = '{schema}'
    AND pg_class.relname = '{table_name}'
ORDER BY
    attnum ASC;