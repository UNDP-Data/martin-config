WITH bbox AS (
    SELECT ST_Transform(ST_SetSRID(ST_Extent({geom_column}), {srid}), 4326) as extent
	FROM {schema}.{table_name}
)
SELECT
    ST_Xmin(bbox.extent) as xmin,
    ST_Ymin(bbox.extent) as ymin,
    ST_Xmax(bbox.extent) as xmax,
    ST_Ymax(bbox.extent) as ymax
FROM bbox;