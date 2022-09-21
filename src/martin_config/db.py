import asyncpg
import logging
from martin_config import utils
from urllib.parse import quote_plus, urlencode
import json

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

logger = logging.getLogger(__name__)
ALLOWED_METHODS = 'execute', 'fetch', 'fetchval'


async def run_query(conn_obj=None, sql_query=None, method='execute'):
    """
    Run a SQL query represented by sql_query against the context represented by conn_obj
    :param conn_obj: asyncpg connection object
    :param sql_query: str, SQL query
    :param method: str, the asyncpg method used to perform the query. One of
    'execute', 'fetch', 'fetch_val', 'fetch_all'
    :return:
    """
    assert conn_obj is not None, f'invalid conn_obj={conn_obj}'
    assert sql_query not in ('', None), f'Invalid sql_query={sql_query}'
    assert method not in ('', None), f'Invalid method={method}'
    assert method in ALLOWED_METHODS, f'Invalid method={method}. Valid option are {",".join(ALLOWED_METHODS)}'
    m = getattr(conn_obj, method)
    return await m(sql_query)


def interpolate_query(sql_file_name=None, **kwargs):
    sql_query_txt = utils.get_sqlfile_content(sql_file_name=sql_file_name)

    assert '{' in sql_query_txt, f'SQL query {sql_query_txt} does not contain python string template vars'
    assert '}' in sql_query_txt, f'SQL query {sql_query_txt} does not contain python string template vars'
    try:
        return sql_query_txt.format(**kwargs)
    except KeyError as ke:
        logger.error(f'SQL query template {sql_query_txt} \nneeds {ke} argument')
        raise
    except  Exception as e:
        logger.error(f'Failed to format SQL query template  {sql_query_txt} using {"".join(kwargs)}')
        raise


async def list_function_sources(conn_obj=None, sql_file_name='func_sources.sql'):
    """
    List functions that can be used by martin
    see https://github.com/maplibre/martin#function-sources

    In short all function that have exactly 4 params (z,x,y,query_params) are returned
    :param conn_obj:
    :param: sql_file_name, str, the name of the SQL file that holds te query
    :return: list of asyncpg.Records for each found function
    """

    sql_query = utils.get_sqlfile_content(sql_file_name=sql_file_name)
    return await run_query(conn_obj=conn_obj, sql_query=sql_query, method='fetch')


async def table_exists(conn_obj=None, sql_file_name='table_exists.sql', table=None):
    """
    Check if a given table exists
    :param conn_obj: instance of asyncpg.connection
    :param sql_file_name:
    :param table_name: str, the name of the table
    :return: True if the table exists false otherwise
    """
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    sql_query = interpolate_query(sql_file_name=sql_file_name, table=table)
    try:
        await run_query(
            conn_obj=conn_obj,
            sql_query=sql_query,
            method='execute'
        )
        return True

    except asyncpg.exceptions.UndefinedTableError:
        return False


async def get_column_comment(conn_obj=None, sql_file_name='get_col_comment.sql', table=None, column=None):
    """
    Get comment for a column of a table
    :param table: str, fully qualified (schema.table_name) table name
    :param column_name: str, column name
    :param conn_obj: instance of asyncpg connection object
    :param sql_file_name, str, the name of SQL file to use to run the query
    :return: None
    """
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    assert column not in ('', None), f'Invalid column={column}'

    schema, table_name = table.split('.')
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        schema=schema,
        table_name=table_name,
        column=column,

    )
    result = await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetchval'
    )
    return dict(urlparse.parse_qsl(result, strict_parsing=False))


async def set_column_comment(conn_obj=None,
                             sql_file_name='set_col_comment.sql',
                             table=None,
                             column=None,
                             value={}
                             ):
    """
    Set comment for a column of a table
    :param table: str, fully qualified (schema.table_name) table name
    :param column: str, column name
    :param value: dict, a dict that will be url encoded and set as string
    :param conn_obj: instance of asyncpg connection object
    :param sql_file_name, str, the name of SQL file to use to run the query
    :return: None
    """
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    assert column not in ('', None), f'Invalid column={column}'
    assert value not in ('', None), f'Invalid value={value}'
    assert value, f'Invalid column comment value {value}'
    try:
        value.items()
    except (TypeError, AttributeError) as e:
        logger.error(f'Column comment value={value} needs to be a mapping. The suplied value is {type(value)}')
        raise

    url_encoded_value = urlencode(value)
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        table=table,
        column=column,
        url_encoded_value=url_encoded_value
    )
    await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='execute'
    )


async def delete_column_comment(conn_obj=None, sql_file_name='delete_col_comment.sql', table=None, column=None):
    """
    Delete comment for a column of a table
    :param table: str, fully qualified (schema.table_name) table name
    :param column: str, column name
    :param conn_obj: instance of asyncpg connection object
    :param sql_file_name, str, the name of SQL file to use to run the query
    :return: None
    """
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    assert column not in ('', None), f'Invalid column={column}'
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        table=table,
        column=column,
    )
    await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='execute'
    )


async def set_table_comment(conn_obj=None,
                            sql_file_name='set_table_comment.sql',
                            table=None,
                            value={}
                            ):
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    assert value not in ('', None), f'Invalid value={value}'
    assert value, f'Invalid comment value {value}'
    try:
        value.items()
    except (TypeError, AttributeError) as e:
        logger.error(f'Table comment value={value} needs to be a mapping. The supplied value is {type(value)}')
        raise
    url_encoded_value = urlencode(value)
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        table=table,
        url_encoded_value=url_encoded_value
    )
    await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='execute'
    )


async def get_table_comment(conn_obj=None, sql_file_name='get_table_comment.sql', table=None):
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        table=table,
    )
    result = await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetchval'
    )
    return dict(urlparse.parse_qsl(result, strict_parsing=False))


async def delete_table_comment(conn_obj=None, sql_file_name='delete_table_comment.sql', table=None):
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        table=table,
    )
    result = await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='execute'
    )
    assert result == 'COMMENT'


async def column_is_publishable(conn_obj=None, table=None, column=None):
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    assert column not in ('', None), f'Invalid column={column}'
    col_comment_dict = await get_column_comment(conn_obj=conn_obj,
                                                table=table,
                                                column=column)
    if not 'publish' in col_comment_dict:
        return
    return eval(col_comment_dict['publish'].lower().capitalize())


async def table_is_publishable(conn_obj=None, table=None, ):
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'

    table_comment_dict = await get_table_comment(conn_obj=conn_obj,
                                                 table=table,
                                                 )
    if not 'publish' in table_comment_dict:
        return False
    return eval(table_comment_dict['publish'].lower().capitalize())


async def drop_table(conn_obj=None, sql_file_name='drop_table.sql', table=None):
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        table=table,
    )
    await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='execute'
    )


async def get_table_columns(conn_obj=None, sql_file_name='get_table_columns.sql', table=None):
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    schema, table_name = table.split('.')
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        schema=schema,
        table_name=table_name,
    )
    return await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetch'
    )


async def get_table_columns_meta(conn_obj=None, sql_file_name='get_table_columns_meta.sql', table=None):
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    schema, table_name = table.split('.')
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        schema=schema,
        table_name=table_name,
    )
    return await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetch'
    )


async def database_exists(dsn=None, **conn_dict):
    """
    Check if a database exists in a given postgres server

    :return: True id the given database exists, False otherwise
    """
    try:
        if dsn is not None:
            assert dsn, f'Invalid dsn={dsn}'
            conn_dict = utils.cs2d(dsn)
        conn = await asyncpg.connect(
            command_timeout=utils.POOL_COMMAND_TIMEOUT, timeout=utils.CONNECTION_TIMEOUT,
            **conn_dict)

        await conn.close()

        return True
    except asyncpg.InvalidCatalogNameError:
        return False


async def list_databases(dsn=None, sql_file_name='list_databases.sql', **conn_dict):
    """
    List databases in a given server

    :return: a tuple with the names of the databases that exists in the postgres server
    """
    if dsn is not None:
        assert dsn, f'Invalid dsn={dsn}'
        conn_dict = utils.cs2d(dsn)
    conn_dict['database'] = 'template1'

    conn = await asyncpg.connect(
        command_timeout=utils.POOL_COMMAND_TIMEOUT,
        timeout=utils.CONNECTION_TIMEOUT,
        **conn_dict
    )

    sql_query = utils.get_sqlfile_content(sql_file_name=sql_file_name)
    res = await run_query(
        conn_obj=conn,
        sql_query=sql_query,
        method='fetch'
    )
    await conn.close()
    return tuple([e['datname'] for e in res])


async def list_schemas(conn_obj=None, sql_file_name='list_schemas.sql'):
    """
    List all available schemas using either the conn-obj or conn_string

    :return: the available schemas
    NB the information_schema and pg_catalog are excluded
    """

    sql_query = utils.get_sqlfile_content(sql_file_name=sql_file_name)
    res = await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetch'
    )

    return tuple(set([e['schema_name'] for e in res]) - set(['information_schema', 'pg_catalog']))


async def list_tables(conn_obj=None, schema=None):
    if schema is not None:
        assert schema != '', f'Invalid schema={schema}'
        available_schemas = await list_schemas(conn_obj=conn_obj)
        assert schema in available_schemas, f'schema "{schema}" does not exist in {conn_obj} '
        sql_file_name = 'list_schema_tables.sql'
        sql_query = interpolate_query(
            sql_file_name=sql_file_name,
            schema=schema,

        )
    else:
        sql_file_name = 'list_tables.sql'
        sql_query = utils.get_sqlfile_content(sql_file_name=sql_file_name)
    res = await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetch'
    )
    return set([f'{e["schemaname"]}.{e["tablename"]}' for e in res])


async def get_bbox(conn_obj=None, table=None, geom_column=None, srid=None, compute_extent=False):
    """
    Conputer the spatial extent of a postgis layer

    :return: the spatial extent (xmin, ymin, xmax, xmax) in LAT/LON, list
    """
    assert table not in ('', None), f'Invalid table={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    assert geom_column not in ('', None), f'Invalid geom_column={geom_column}'

    schema, table_name = table.split('.')
    if not compute_extent:
        logger.debug(f'using estimated extent')
        sql_file_name = 'get_estimated_bbox.sql'
    else:
        sql_file_name = 'get_computed_bbox.sql'
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        schema=schema,
        table_name=table_name,
        geom_column=geom_column,
        srid=srid
    )
    res = await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetch'
    )
    assert len(res) == 1, f'Failed to compute spatial extent for table {table}'
    res = res[0]
    return res['xmin'], res['ymin'], res['xmax'], res['ymax']


async def get_table_cfg(conn_obj=None, table=None):
    """
    Create a config dictionary suitable for configuring martin vector tile service
     for a given table located  in  a database defined by conn_obj or conn_dict

    :return: dict with the configuration as per https://github.com/urbica/martin#configuration-file
    NB: martin support tables/layers with more than one geometry column. This is desirabvle because
    the web mercator projectin EPSG 3857 should be used for displaying and is not suitable for
    performing spatial computations
    """
    assert '.' in table, f'Invalid table={table}. Needs to be fully qualified: schema.table_name'
    schema, table_name = table.split('.')

    logger.info(f'Creating configuration for {table}')

    columns = await get_table_columns(conn_obj=conn_obj, table=table)
    if not columns:
        logger.info(
            f'Skipping table {table}. No columns detected')
        return
    srid = None
    for r in columns:  # prefer/pick the 3857 geom or the last in case 3857 is not used and more than 1
        srid = r['srid']
        geom_column = r['geom_column']
        geom_type = r['type']
        properties = r['properties']
        if srid == 3857:
            break
    tbl_dict = dict(id=table, schema=schema, table=table_name)

    try:
        bb = await get_bbox(
            conn_obj=conn_obj,
            table=table,
            geom_column=geom_column,
            srid=srid,
            compute_extent=False
        )
    except Exception as e:
        logger.error(
            f'Failed to fetch bounding box for {table} because {e}. Skipping...')
        return

    tbl_dict['bounds'] = list(bb)
    tbl_dict['srid'] = srid
    tbl_dict['geometry_column'] = geom_column
    tbl_dict['id_column'] = '~'

    tbl_dict['extent'] = 4096
    tbl_dict['buffer'] = 64
    tbl_dict['geometry_type'] = geom_type
    tbl_dict['clip_geometry'] = True
    properties = json.loads(properties)
    props = {}
    # properties/attributes are  eagerly collected and are skipped only
    # is the column is marked with publish=False
    for k, v in properties.items():
        col_is_publishable = await column_is_publishable(conn_obj=conn_obj, table=table, column=k)
        if col_is_publishable is False:
            logger.debug(
                f'Column {k} from {table} is not publishable and will not be included in the config')
            continue
        props[k] = v

    tbl_dict['properties:'] = props

    return {f'{table}:': tbl_dict}


async def create_config_dict(dsn=None, schemas=None, **conn_dict):
    """
    Create a configuration dictionary for all table sources in a postgis database.
    If schema is provided the config is generated for the given schema
    :param schemas: iter of strings representing schemas in db
    :return: a dictionary with configuration for every layer
    """

    if conn_dict and not dsn:
        dsn = utils.cd2s(**conn_dict)
    assert dsn not in ('', None), f'Invalid dsn={dsn}'

    schemas_cfg = {}
    if schemas:
        logger.info(
            f'Creating config dict for tables in \"{", ".join(schemas)}" schemas ...')
    else:
        logger.info(
            f'Creating config dict for tables in all available schemas ...')

    async with asyncpg.create_pool(dsn=dsn, min_size=utils.POOL_MINSIZE, max_size=utils.POOL_MAXSIZE,
                                   command_timeout=utils.POOL_COMMAND_TIMEOUT, ) as pool:
        logger.debug('Connecting to database...')
        async with pool.acquire(timeout=utils.CONNECTION_TIMEOUT) as conn_obj:
            available_schemas = await list_schemas(conn_obj=conn_obj)
            available_schemas = set(available_schemas) - set(['public'])
            if not schemas:
                schemas = available_schemas
            for schema in schemas:

                logger.debug(f'Checking if schema {schema} exists')
                if not schema in available_schemas:
                    logger.warning(f'Schema "{schema}" does not exist in {dsn}.'
                                   f'Valid options are: {",".join(available_schemas)}')
                else:
                    for table in await list_tables(conn_obj=conn_obj, schema=schema):
                        try:
                            will_publish = await table_is_publishable(table=table, conn_obj=conn_obj)
                        except Exception as ee:
                            logger.error(
                                f'Failed to fetch comments for table {table} because {ee}. Skipping...')
                            continue
                        if will_publish == False:
                            logger.info(
                                f'{table} was marked as not publishable and will be skipped')
                            continue
                        table_cfg = await get_table_cfg(conn_obj=conn_obj, table=table)
                        if table_cfg:
                            schemas_cfg.update(table_cfg)

            funcs_cfg = {}
            funcs = await list_function_sources(conn_obj=conn_obj,)
            for func_rec in funcs:
                func_name = f'{func_rec["function_schema"]}.{func_rec["function_name"]}'
                funcs_cfg[f'{func_name}:'] = {
                    'id': func_name,
                    'schema': func_rec['function_schema'],
                    'function': func_rec['function_name'],
                    'minzoom': 0,
                    'maxzoom': 22,
                    'bounds': [-180.0, -90.0, 180.0, 90.0],
                }

    return {'table_sources:': schemas_cfg , 'function_sources:': funcs_cfg}


def create_general_config(listen_addresses="'0.0.0.0:3000'", connection_string="'$DATABASE_URL'",
                          pool_size=20, keep_alive=75, woker_processes=8, watch=False,
                          danger_accept_invalid_certs=True,
                          ):
    """
    Creates the general section of the configuration for martin VT server
    :param listen_address: The socket address to bind to env variable '$LISTEN_ADDRESSES'
    :param connection_string: Database connection string, bind to  env variable '$DATABASE_URL'
    :param pool_size: Maximum connections pool size [default: 20]
    :param keep_alive: Connection keep alive timeout [default: 75]
    :param woker_processes: Number of web server workers [default 8]
    :param watch: Enable watch mode, default False
    :param danger_accept_invalid_certs: Trust invalid certificates. This introduces significant vulnerabilities, and
            should only be used as a last resort. default True
    :return: dict
    """
    return locals()
