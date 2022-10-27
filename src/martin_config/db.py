import asyncpg
import logging
from martin_config import utils
from urllib.parse import urlencode
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
    """
    Given a SQL script containinig python string formatting variables
    interpolate the items supplied in kwargs into the SQL script
    :param sql_file_name: str, the name of the templated SQL script. All files are
    located in the sql folder
    :param kwargs: dict containing items ot be interpolated
    :return: interpolated SQL script
    """
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


async def list_function_sources(conn_obj=None, sql_file_name='func_sources.sql', schema=None):
    """
    List functions that can be used by martin
    see https://github.com/maplibre/martin#function-sources

    In short all function that have exactly 4 params (z,x,y,query_params) are returned
    :param conn_obj: instance of asyncpg.connection
    :param: sql_file_name, str, the name of the SQL file that holds te query
    :return: list of asyncpg.Records for each found function
    """

    assert schema is not None, f'Invalid schema={schema}'

    sql_query = interpolate_query(sql_file_name=sql_file_name, schema=schema)
    return await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetch'
    )


async def table_exists(conn_obj=None, sql_file_name='table_exists.sql', table=None):
    """
    Check if a given table exists
    :param conn_obj: instance of asyncpg.connection
    :param sql_file_name: str, the name of the SQL template script
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
    :param column: str, column name
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
    """
    Set the comment for a table.
    The comment value is a dict and before being written it is urlencoded.

    :param conn_obj: instance of asyncpg.connection
    :param sql_file_name: str, the name of the SQL template
    :param table:str, the fully qualified name of the table
    :param value: dict
    :return: None
    NB. the comments are meant to be used as control/decision structures.
    For example {'publish': True} will be interpreted  as publishable
    """
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
    """
        Fetch the comment for a table.
        The comment value is a dict and before being returned it is urldecoded.
        In case the comment contains other string characters, only the part that is interpreted
        as a query string is returned

        :param conn_obj: instance of asyncpg.connection
        :param sql_file_name: str, the name of the SQL template
        :param table:str, the fully qualified name of the table

        :return: dict representing the urldecoded query string extracted from the comment
        NB. the comments are meant to be used as control/decision structures.
        For example {'publish': True} will be interpreted  as publishable
    """
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
    """
        Delete  the comment for a table.


        :param conn_obj: instance of asyncpg.connection
        :param sql_file_name: str, the name of the SQL template
        :param table:str, the fully qualified name of the table

        :return: None
        NB. the comments are meant to be used as control/decision structures.
        For example {'publish': True} will be interpreted  as publishable
    """
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
    """
    Check if a column is publishable by extracting the column comment and
    treating  it as a query string and interpreting its meaning.
    If the comment contains the value publish=True the column will
    be included in the config file

    :param conn_obj: instance of asyncpg.connection
    :param table: str, the name of the table
    :param column: str, the name of the column
    :return: None if the keyword publish does not exist in the column comment
             True | False, depending on the value set in the publish keyword

    """
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    assert column not in ('', None), f'Invalid column={column}'
    col_comment_dict = await get_column_comment(conn_obj=conn_obj,
                                                table=table,
                                                column=column)
    if not 'publish' in col_comment_dict:
        return
    return eval(col_comment_dict['publish'].lower().capitalize())


async def column_is_accessible(conn_obj=None, sql_file_name='column_is_accessible.sql', table=None, user=None, column=None):
    """
    Check if a column is publishable by extracting the column comment and
    treating  it as a query string and interpreting its meaning.
    If the comment contains the value publish=True the column will
    be included in the config file

    :param conn_obj: instance of asyncpg.connection
    :param table: str, the name of the table
    :param column: str, the name of the column
    :return: None if the keyword publish does not exist in the column comment
             True | False, depending on the value set in the publish keyword

    """
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    assert user not in ('', None), f'Invalid user={column}'
    assert column not in ('', None), f'Invalid column={column}'

    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        user=user,
        table=table,
        column=column
    )

    return await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetchval'
    )
async def function_is_publishable(conn_obj=None, sql_file_name='function_is_accessible.sql',
                                  user=None, function_name=None, schema=None):

    assert user not in ('', None), f'Invalid user={user}'
    assert function_name not in ('', None), f'Invalid function_name={function_name}'
    assert schema not in ('', None), f'Invalid schema={schema}'

    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        function_name=function_name,
        schema=schema,
        user=user,

    )

    return await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetchval'
    )



async def schema_is_accessible(conn_obj=None, sql_file_name='schema_is_accessible.sql', schema=None,  user=None ):
    """
        Checks is a user has usage privilege on a given schema
        :param conn_obj: instance of asyncpg.connection
        :param sql_file_name: str, the name of the SQL template
        :param schema: str, the schema
        :param user: str, the suer
        :return: True|False
    """

    assert schema not in ['', None], f'Invalid schema {schema}'
    assert user not in ['', None], f'Invalid user {schema}'

    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        schema=schema,
        user=user,

    )

    return await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetchval'
    )


async def table_is_accessible(conn_obj=None, sql_file_name='table_is_accessible.sql', user=None, table=None, ):
    """
    Check if a table is publishable by checking if the given user has
    been granted select rights on the table

    :param conn_obj: instance of asyncpg.connection
    :param user: str, the name of the user
    :param table: str, the name of the table
    :return: True if the user has been granted select rights, False otherwise

    """
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        user=user,
        table=table,
    )

    return await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetchval'
    )



async def table_is_publishable(conn_obj=None, table=None, ):
    """
    Check if a table is publishable by extracting the table comment and
    treating  it as a query string and interpreting its meaning.
    If the comment contains the value publish=True the table will
    be included in the config file

    :param conn_obj: instance of asyncpg.connection
    :param table: str, the name of the table
    :return: None if the keyword publish does not exist in the column comment
             True | False, depending on the value set in the publish keyword

    """
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'

    table_comment_dict = await get_table_comment(conn_obj=conn_obj,
                                                 table=table,
                                                 )
    if not 'publish' in table_comment_dict:
        return False
    return eval(table_comment_dict['publish'].lower().capitalize())


async def drop_table(conn_obj=None, sql_file_name='drop_table.sql', table=None):
    """
    Removes a given table from the database encapsulated into the conn_obj
    :param conn_obj: instance of asyncpg_connection
    :param sql_file_name: str, the name of the SQL template script
    :param table: str, fully qualified name of the table to be removed
    :return: None
    """
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

async def get_table_primary_key(conn_obj=None, sql_file_name='get_table_primary_key.sql', table=None):
    assert table not in ('', None), f'Invalid table_name={table}'
    assert '.' in table, f'table={table} is not fully qualified'
    schema, table_name = table.split('.')
    sql_query = interpolate_query(
        sql_file_name=sql_file_name,
        table_name=table_name
    )
    return await run_query(
        conn_obj=conn_obj,
        sql_query=sql_query,
        method='fetch'
    )

async def get_table_columns(conn_obj=None, sql_file_name='get_table_columns.sql', table=None):
    """
    Fetch fundamental  info about a given table:
        name of the geom column
        SRID
        geom type
         a mapping/JSON where  the  keys are column names and values are the column types (postgres)

    Ex: <Record
            schema='zambia'
            table_name='poverty'
            geom_column='geom'
            srid=3857
            type='MULTIPOLYGON'
            properties='{"id": "int4", "ctr": "float8", "fid": "int8", "cst_n": "varchar", "ctr_n": "varchar", "cst_n_1": "varchar", "district": "varchar", "gred_cst": "float8", "num_poor": "float8", "province": "varchar", "std_error": "float8", "constitu_1": "float8", "constituen": "varchar", "districtco": "float8", "povertyhea": "float8", "provinceco": "float8"}'
        >
    This query fuels the creation of a config for any given table
    :param conn_obj: instanc eof asyncpg.connection
    :param sql_file_name: str, the name of the SQL template script
    :param table: str, fully qualified table name
    :return: list with one asyncpg record
    """
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
    :param dsn, str, Postgres DSN string
    :param conn_dict, dict with items representing info to connect to the server
    :return: True if the given database exists, False otherwise
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
    :param dsn, str, Postgres DSN string
    :param sql_file_name, str, the name of the SQL template
    :param conn_dict, dict with items representing info to connect to the server

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
    :param conn_obj, instance of asyncpg.connection
    :param sql_file_name, str, the name of the SQL template
    :return: a tuple with the available schemas
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
    """
    Lists the tables in the database &| schema
    :param conn_obj: instance of asyncpg.connection
    :param schema: str, the name of the schema where the tables will be listed
    :return: set of the fully qualified names of the found tables
    """
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
    Compute the spatial extent of a postgis layer
    :param conn_obj: instance of asyncpg.connection
    :param table: str, fully qualified table name
    :param geom_column: str, the name of the geom column  to use
    :param srid: int, the PostGIS SRID of the table
    :param compute_extent: bool, flag to indicate if the extent will be computed or estimated

    :return: the spatial extent (xmin, ymin, xmax, xmax) in  geographic coordinates LAT/LON


    """
    assert srid is not None, f'Invalid srid={srid}'
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


async def get_table_cfg(conn_obj=None, user=None, table=None):
    """
    Create a config dictionary suitable for configuring martin vector tile service
     for a given table located  in  a database defined by conn_obj or conn_dict
    :param: conn_obj, instance of asyncpg.connection
    :param table: str, fully qualified table name
    :return: dict with the configuration as per https://github.com/urbica/martin#configuration-file
    NB: martin support tables/layers with more than one geometry column. This is desirable because
    the web mercator projection EPSG 3857 should be used for displaying and is not suitable for
    performing certain spatial computations
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

    try:
        prim_key_rec = await get_table_primary_key(conn_obj=conn_obj,table=table)
    except Exception as e:
        logger.info(f'Features in table {table} will not have feature id .')
        prim_key_rec = []

    tbl_dict['bounds'] = list(bb)
    tbl_dict['srid'] = srid
    tbl_dict['geometry_column'] = geom_column
    if prim_key_rec:
        pkey_rec = prim_key_rec[0]
        pkey_name = pkey_rec['column_name']
        pkey_type = pkey_rec['data_type']
        if not 'int' in pkey_type:
            logger.warning(f'table {table} has a non number primary key')
        tbl_dict['id_column'] = pkey_name
    else:
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

        if not user:
            col_is_publishable = await column_is_publishable(conn_obj=conn_obj, table=table, column=k)
        else:
            col_is_publishable = await column_is_accessible(conn_obj=conn_obj,table=table,user=user,column=k)
        if col_is_publishable is False:
            logger.debug(
                f'Column {k} from {table} is not publishable and will not be included in the config')
            continue
        props[k] = v

    tbl_dict['properties:'] = props

    return {f'{table}:': tbl_dict}

