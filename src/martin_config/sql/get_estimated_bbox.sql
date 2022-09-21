 WITH bbox AS (
     SELECT ST_Transform(
 			ST_SetSRID(
 				ST_EstimatedExtent('{schema}', '{table_name}', '{geom_column}'), {srid}),
 			4326)
 	as extent
 )
 SELECT
     ST_Xmin(bbox.extent) as xmin,
     ST_Ymin(bbox.extent) as ymin,
     ST_Xmax(bbox.extent) as xmax,
     ST_Ymax(bbox.extent) as ymax
 FROM bbox;