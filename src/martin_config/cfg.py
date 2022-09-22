import logging
import os

import asyncpg
import asyncio
import json
import socket
import yaml
import time
from urllib.parse import quote_plus, urlencode
from martin_config.utils import CWD
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse


logger = logging.getLogger()


"""
This module provides asyncpg based functionality  to interact with the postgres database for the purpopse managing
martin server
asyncpg is simpler, faster and more modern than psycopg2.
Almost all function accept either an already connected object (conn_obj) or a conn_dict argument
to do their job.
The most important reason to use asyncpg is the fact that  if the Pool object is used to manage the connections
 a connection timeout can be specified  so the functions  do not fail instantly if a pool that has already connected
 to postgres fails to acquire a connection. This can be the case if this script  is interacting with a remote database
 and the network layer fails for a couple of seconds.
NB: Note the connection timeout does not imply of the server is down, the server has to be reachable.
"""

POOL_COMMAND_TIMEOUT = 15 * 60  # seconds
POOL_MINSIZE = 3
POOL_MAXSIZE = 5
CONNECTION_TIMEOUT = 30


async def set_column_comment(table_name=None, column_name=None, value={}, conn_obj=None, **conn_dict):
    """
    Set comment for a column of a table
    :param table_name: str, fully qualified (schema.table_name) table name
    :param column_name: str, column name
    :param value: dict, a dict that will be url endoced and set as string
    :param conn_obj: instance of asyncpg connection object
    :param conn_dict: dict holding the connection params
    :return: None
    """
    v = urlencode(value)

    qtext = f"COMMENT ON COLUMN {table_name}.{column_name} IS '{v}';"
    try:
        if conn_obj is not None:
            val = await conn_obj.execute(qtext)
            assert val == 'COMMENT'

        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                    val = await conn_obj.execute(qtext)
                    assert val == 'COMMENT'

    except Exception as e:
        raise


async def delete_column_comment(table_name=None, column_name=None,  conn_obj=None, **conn_dict):
    """
    Deletes a the comment of a column from a table
    :param table_name: str, fully qualified table name
    :param column_name: str, the name of the column whose comment will be deleted
    :param conn_obj: instance of asyncpg connrection object
    :param conn_dict: dict holding the connection params
    :return: None
    """
    qtext = f"COMMENT ON COLUMN {table_name}.{column_name} IS NULL;"
    try:
        if conn_obj is not None:
            val = await conn_obj.execute(qtext)
            assert val == 'COMMENT'
        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                    val = await conn_obj.execute(qtext)
                    assert val == 'COMMENT'

    except Exception as e:
        raise


async def get_column_comment(table_name=None, column_name=None, conn_obj=None, **conn_dict):
    """
    Retrieves the comment set on a column of a table
    :param table_name: str, fully qualified table name
    :param column_name: str, the name of the column whose comment will be deleted
    :param conn_obj: instance of asyncpg connrection object
    :param conn_dict: dict holding the connection params
    :return: None
    """

    schema, tname = table_name.split('.')

    qtext = f"""
            SELECT
                pgd.description FROM pg_catalog.pg_statio_all_tables AS st
            INNER JOIN pg_catalog.pg_description pgd ON (pgd.objoid=st.relid)
            INNER JOIN information_schema.columns c ON (pgd.objsubid=c.ordinal_position AND c.table_schema=st.schemaname AND c.table_name=st.relname AND c.table_name = '{tname}' AND c.table_schema = '{schema}' AND c.column_name = '{column_name}');
    """

    try:
        if conn_obj is not None:
            val = await conn_obj.fetchval(qtext)
        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                    val = await conn_obj.fetchval(qtext)
        return dict(urlparse.parse_qsl(val, strict_parsing=False))
    except Exception as e:
        raise


async def set_table_comment(table_name=None, value={}, conn_obj=None, **conn_dict):
    """
    Set comment on table as query string from a dict
    :param table_name: str, the table name to check
    :param value, dict, the values to encode as query string into the comment
    :param conn_obj: instance of asyncpg connection
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return: None
    """
    v = urlencode(value)
    qtext = f"COMMENT ON TABLE {table_name} IS '{v}';"
    logger.debug(qtext)
    try:
        if conn_obj is not None:
            val = await conn_obj.execute(qtext)
            assert val == 'COMMENT'

        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                    val = await conn_obj.execute(qtext)
                    assert val == 'COMMENT'

    except Exception as e:
        raise


async def delete_table_comment(table_name=None, conn_obj=None, **conn_dict):
    """
     Delete comment from a table
    :param table_name: str, the table name to check
    :param conn_obj: instance of asyncpg connection
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return: None
    """

    qtext = f"COMMENT ON TABLE {table_name} IS NULL;"
    logger.debug(qtext)
    try:
        if conn_obj is not None:
            val = await conn_obj.execute(qtext)
            assert val == 'COMMENT'

        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                    val = await conn_obj.execute(qtext)
                    assert val == 'COMMENT'

    except Exception as e:
        raise


async def get_table_comment(table_name=None, conn_obj=None, **conn_dict):
    """
    Retrieves the comment for a table as a dict (name:value) pairs
    :param table_name: str, the table name to check
    :param conn_obj: instance of asyncpg connection
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return:
    """

    qtext = f"SELECT obj_description('{table_name}'::regclass) AS comment;"
    logger.debug(qtext)
    try:
        if conn_obj is not None:
            comment_val = await conn_obj.fetchval(qtext)
            return dict(urlparse.parse_qsl(comment_val))

        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                    comment_val = await conn_obj.fetchval(qtext)
                    return dict(urlparse.parse_qsl(comment_val))

    except Exception as e:
        raise


async def column_is_publishable(table_name=None, column_name=None, conn_obj=None, **conn_dict):
    if conn_obj is not None:
        col_comment_dict = await get_column_comment(table_name=table_name, column_name=column_name, conn_obj=conn_obj, **conn_dict)
        if not 'publish' in col_comment_dict:
            return
        return eval(col_comment_dict['publish'].lower().capitalize())

    else:
        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                col_comment_dict = await get_column_comment(table_name=table_name, column_name=column_name, conn_obj=conn_obj, **conn_dict)
                if not 'publish' in col_comment_dict:
                    return
                return eval(col_comment_dict['publish'].lower().capitalize())


async def table_is_publishable(table_name=None, conn_obj=None, **conn_dict):
    """
    Check is a table was set to be published through comments
    :param table_name:  str, the name of the table
    :param conn_obj: instance of asyncpg.Connection
    :param conn_dict: dict holding conn params
    :return: None is the publish was not set on the comment
    """
    if conn_obj is not None:
        comment_dict = await get_table_comment(table_name=table_name, conn_obj=conn_obj, **conn_dict)
        if not 'publish' in comment_dict:
            return False
        return eval(comment_dict['publish'].lower().capitalize())

    else:
        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                comment_dict = await get_table_comment(table_name=table_name, conn_obj=conn_obj, **conn_dict)
                if not 'publish' in comment_dict:
                    return False
                return eval(comment_dict['publish'].lower().capitalize())


def atimeit(func):

    async def process(func, *args, **params):
        return await func(*args, **params)

    async def helper(*args, **params):
        start = time.time()
        result = await process(func, *args, **params)
        elapsed = time.time() - start
        logger.info(f'{func.__name__} took {elapsed} secs')
        return result

    return helper


def cd2s(user=None, password=None, host=None, port=None, database=None, **kwargs):
    """
    Convert a dict representing a conn dict into a connection string
    :param user:
    :param password:
    :param host:
    :param port:
    :param database:
    :param kwargs: any other params as key=value. They will be added to the end as options
    :return:
    """
    if 'ssl' in kwargs:
        kwargs['sslmode'] = kwargs['ssl']
        del kwargs['ssl']
    try:
        return f'postgres://{quote_plus(user)}:{quote_plus(password)}@{quote_plus(host)}:{port}/{database}?{urlencode(kwargs)}'
    except KeyError:
        raise


def cs2d(url=None):
    """
    Convert a connection string to dict
    :param url:
    :return:
    """

    # otherwise parse the url as normal
    config = {}

    url = urlparse.urlparse(url)

    # Split query strings from path.
    path = url.path[1:]
    if '?' in path and not url.query:
        path, query = path.split('?', 2)
    else:
        path, query = path, url.query

    # Handle postgres percent-encoded paths.
    hostname = url.hostname or ''
    if '%2f' in hostname.lower():
        # Switch to url.netloc to avoid lower cased paths
        hostname = url.netloc
        if "@" in hostname:
            hostname = hostname.rsplit("@", 1)[1]
        if ":" in hostname:
            hostname = hostname.split(":", 1)[0]
        hostname = hostname.replace('%2f', '/').replace('%2F', '/')

    port = url.port
    if query:
        if 'sslmode' in query:
            query = query.replace('sslmode', 'ssl')
        d = dict(urlparse.parse_qsl(query))

        config.update(d)

    # Update with environment configuration.
    config.update({
        'database': urlparse.unquote(path or ''),
        'user': urlparse.unquote(url.username or ''),
        'password': urlparse.unquote(url.password or ''),
        'host': hostname,
        'port': port or '',

    })

    return config


async def table_exists(table_name=None, conn_obj=None, **conn_dict):
    """
    Check whetehr table_name in the given postgres database
    :param table_name: str, the table name to check
    :param conn_obj: instance of asyncpg connection
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return: bool, True if the table exists False otherwise
    """
    try:
        if conn_obj is not None:
            val = await conn_obj.fetch(f'SELECT * from {table_name} LIMIT 1')
            return True
        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                    val = await conn_obj.fetch(f'SELECT * from {table_name} LIMIT 1')
                    return True
    except asyncpg.exceptions.UndefinedTableError:
        return False


async def drop_table(table_name=None, conn_obj=None, **conn_dict):
    """
    Removes the table_name from the database represented either through the conn_obj object
    or a dict used to connect to a postgres database
    :param table_name: str, the name of the tabel to be deleted
    :param conn_obj: instance of asyncpg.Connection
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return: None
    """

    qtext = f'DROP TABLE IF EXISTS {table_name};'
    if conn_obj is not None:
        async with conn_obj.transaction():
            res = await conn_obj.execute(qtext)
            logger.info(f'DROPPED TABLE {table_name}')
    else:
        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                async with conn_obj.transaction():
                    res = await conn_obj.execute(qtext)
                    logger.info(f'DROPPED TABLE {table_name}')


async def delete_from_table(table_name=None, conn_obj=None, start_dfb=None, end_dfb=None, start_slot=None, end_slot=None, **conn_dict):
    """
    Delete data from the table_name. Assumes the table has start_df, end_dfb, startt_slot, end_slot columns
    :param table_name: str, table name
    :param conn_obj: instance of asyncpg.Connection
    :param start_dfb: int
    :param end_dfb: int
    :param start_slot: int,
    :param end_slot: int
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return: int, the number of deleted records
    """
    sql = f'DELETE from {table_name} WHERE dfb >= {start_dfb} AND dfb <= {end_dfb} AND slot >= {start_slot} AND slot <= {end_slot}'
    if conn_obj is not None:
        async with conn_obj.transaction():
            res = await conn_obj.execute(sql)
            logger.info(f'{res} records were deleted from table {table_name}')
    async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                   command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
        async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
            async with conn_obj.transaction():
                res = await conn_obj.execute(sql)
                logger.info(
                    f'{res} records were deleted from table {table_name}')


async def get_columns(table_name=None, conn_obj=None, **conn_dict):
    """
    Fetched the columns of the table_name accessed through the conn_obj arg
    :param table_name: str
    :param conn_obj: and insdtance of asyncpg.Connection
    :return: an iterable of asyncpg.records for each column of the table_name table
    """
    assert '.' in table_name, f'Invalid table_name={table_name}'
    schema, tname = table_name.split('.')

    qtext = f"""
                WITH columns AS (
                  SELECT
                    ns.nspname AS table_schema,
                    class.relname AS table_name,
                    attr.attname AS column_name,
                    trim(leading '_' from tp.typname) AS type_name
                  FROM pg_attribute attr
                    JOIN pg_catalog.pg_class AS class ON class.oid = attr.attrelid
                    JOIN pg_catalog.pg_namespace AS ns ON ns.oid = class.relnamespace
                    JOIN pg_catalog.pg_type AS tp ON tp.oid = attr.atttypid
                  WHERE NOT attr.attisdropped AND attr.attnum > 0
                  )
                SELECT
                  f_table_schema as schema, f_table_name as table_name, f_geometry_column as geom_column, srid, type,
                    COALESCE(
                      jsonb_object_agg(columns.column_name, columns.type_name) FILTER (WHERE columns.column_name IS NOT NULL AND columns.type_name NOT LIKE '%geometry%'),
                      '{{}}'::jsonb
                    ) as properties
                FROM geometry_columns
                LEFT JOIN columns ON
                  geometry_columns.f_table_schema = columns.table_schema AND
                  geometry_columns.f_table_name = columns.table_name AND
                  geometry_columns.f_geometry_column != columns.column_name
                WHERE columns.table_schema = '{schema}' AND columns.table_name = '{tname}'
                GROUP BY f_table_schema, f_table_name, f_geometry_column, srid, type;
            """

    if conn_obj is None:
        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn:
                return await conn.fetch(qtext)
    else:
        return await conn_obj.fetch(qtext)


async def get_columns2(table_name=None, conn_obj=None, **conn_dict):
    """
    Fetched the columns of the table_name accessed through the conn_obj arg
    :param table_name: str, fully qualified: schema.table
    :param conn_obj: and insdtance of asyncpg.Connection
    :return: an iterable of asyncpg.records for each column of the table_name table
    """
    assert '.' in table_name, f'Invalid table_name={table_name}'
    schema, tname = table_name.split('.')

    qtext = f'SELECT' \
            f'  pg_attribute.attname AS column_name,\n' \
            f'  pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type,\n' \
            f"  tp.typname AS type_name\n" \
            f"FROM" \
            f"  pg_catalog.pg_attribute\n" \
            f"INNER JOIN" \
            f"  pg_catalog.pg_class ON pg_class.oid = pg_attribute.attrelid\n" \
            f"INNER JOIN" \
            f"  pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace\n" \
            f"INNER JOIN" \
            f"  pg_catalog.pg_type AS tp ON tp.oid = pg_attribute.atttypid\n" \
            f"WHERE\n" \
            f"  pg_attribute.attnum > 0" \
            f"  AND NOT pg_attribute.attisdropped" \
            f"  AND pg_namespace.nspname = '{schema}'" \
            f"  AND pg_class.relname = '{tname}'\n" \
            f"ORDER BY\n" \
            f"  attnum ASC;"

    if conn_obj is None:
        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn:
                return await conn.fetch(qtext)
    else:
        return await conn_obj.fetch(qtext)


async def database_exists(host=None, user=None, password=None, database=None, **kwargs):
    """
    Check if a database exists in a given postgres server
    :param host: str,  host
    :param user: str, user
    :param password: str, password
    :param database: str database to be checked
    :return: True id the given database exists, False otherwise
    """
    try:
        conn = await asyncpg.connect(host=host, user=user, password=password, database=database,
                                     command_timeout=POOL_COMMAND_TIMEOUT, timeout=CONNECTION_TIMEOUT,
                                     **kwargs)
        await conn.close()
        return True
    except asyncpg.InvalidCatalogNameError:
        return False


async def list_databases(host=None, user=None, password=None, **kwargs):
    """
    List databases in a given server
    :param host: str, host
    :param user: str, user
    :param password: str password
    :param kwargs:
    :return: a tuple with the nmaes of the databases that exists in the postgres server
    """
    if 'database' in kwargs:
        kwargs.pop('database')

    qtext = 'SELECT datname FROM pg_database WHERE datistemplate = false;'
    conn = await asyncpg.connect(host=host, user=user, password=password, database='template1',
                                 command_timeout=POOL_COMMAND_TIMEOUT, timeout=CONNECTION_TIMEOUT **
                                                                               kwargs)

    res = await conn.fetch(qtext)
    await conn.close()
    return tuple([e['datname'] for e in res])

async def list_function_sources(conn_obj=None, **conn_dict):
    """
    List
    :param conn_obj:
    :param conn_dict:
    :return:
    """

    if conn_obj is None:
        assert conn_dict, f'Invalid connection dict {conn_dict} supplied'
    with open(os.path.join(cwd,'sql/func_sources.sql')) as sqlf:
        sql_query = sqlf.read()
        if conn_obj is not None:
            async with conn_obj.transaction():
                func_src_recs = await conn_obj.fetch(sql_query)
                logger.info(f'{len(func_src_recs)} functions were found')
        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                    async with conn_obj.transaction():
                        func_src_recs = await conn_obj.fetch(sql_query)
                        logger.info(f'{len(func_src_recs)} functions were found')

async def fast_insert(table_name=None, records=None, timeout=None, conn_obj=None, **conn_dict):
    """
    Binary fast insert (copy) or records to a table
    :param table_name: str, table name
    :param records: iter of records, each record corresponds to a recprd in the table. A value gas to be present for
            each column
    :param timeout: int, the timeout for this operation
    :param conn_obj: an instance of asyncpg.Conenction
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return: None because the asyncpg copy_records_to_table discards any returned value by the server
    """
    database = conn_dict['database']
    host = conn_dict['host']
    db_exists = await database_exists(**conn_dict)
    if not db_exists:
        databases = tuple([d['datname'] for d in await list_databases(**conn_dict)])
        emess = f'database={database} does not exist on host {host}. Available databases are {databases}'
        raise Exception(emess)

    if conn_obj is not None:
        tables = [e['tablename'] for e in await list_tables(conn_obj=conn_obj)]
        if not table_name in tables:
            raise Exception(
                f'Table "{table_name}" does not exist in database "{database}" on host "{host}"')
        async with conn_obj.transaction():
            nrec = await conn_obj.copy_records_to_table(table_name, records=records, timeout=timeout)

    else:
        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn:
                tables = [e['tablename'] for e in await list_tables(conn)]
                if not table_name in tables:
                    raise Exception(
                        f'Table "{table_name}" does not exist in database "{database}" on host "{host}"')
                async with conn.transaction():
                    nrec = await conn.copy_records_to_table(table_name, records=records, timeout=timeout)


async def upsert(table_name=None, records=None, timeout=None, conn_obj=None, **conn_dict):
    """
    Smart insert of individual records to a  table in a given postgtres server.
    Smart  = if record exists it is updated
    This function uses the ON CONFLICT version of the POSTGRES INSERT to do the job.
    NB: This function  works only for Postgres 9.5 and beyond
    :param table_name: str, the name of the table
    :param records: an iter of records, each record corresponds to a recprd in the table. A value gas to be present for
            each column
    :param timeout: int the timeout in seconds for thi command
    :param conn_obj: an instance of asyncpg.Connection
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return: None
    """
    if conn_obj is None:
        assert 'host' in conn_dict
        host = conn_dict['host']
    else:
        host = conn_obj._addr[0]
    database = conn_dict['database']
    try:
        db_exists = await database_exists(**conn_dict)
        if not db_exists:
            databases = tuple([d['datname'] for d in await list_databases(**conn_dict)])
            emess = f'database={database} does not exist on host {host}. Available databases are {databases}'
            raise Exception(emess)

        if conn_obj is not None:
            version_info = conn_obj.get_server_version()
            if version_info.major < 10 and version_info.minor < 5:
                raise Exception(
                    f'Cannot use upsert on server {host}. Remote Postgres server version is {version_info} ')
            tables = [e['tablename'] for e in await list_tables(conn_obj=conn_obj)]
            if not table_name in tables:
                raise Exception(
                    f'Table "{table_name}" does not exist in database "{database}" on host "{host}"')
            cols = await get_columns2(table_name=table_name, conn_obj=conn_obj)
            col_names = list()
            upsert_col_fixes = list()
            for c in cols:
                col_name = c["column_name"]
                col_names.append(col_name)
                upsert_col_fixes.append(f'{col_name}=EXCLUDED.{col_name}')
            async with conn_obj.transaction():
                sql = f'INSERT INTO {table_name}({", ".join(col_names)})' \
                      f'\nVALUES ({", ".join([f"${i + 1}" for i in range(len(col_names))])})' \
                      f'\nON CONFLICT ON CONSTRAINT {table_name}_pkey DO UPDATE' \
                      f'\nSET {", ".join(upsert_col_fixes)}'

                await conn_obj.executemany(sql, records, timeout=timeout)
                logger.debug(
                    f'{len(records)} records were written to database for channel {records[0][6]}')
        else:
            async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                           command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
                async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn:
                    version_info = conn.get_server_version()
                    if version_info.major < 10 and version_info.minor < 5:
                        raise Exception(
                            f'Cannot use upsert on server {host}. Remote Postgres server version is {version_info} ')
                    tables = [e['tablename'] for e in await list_tables(conn)]
                    if not table_name in tables:
                        raise Exception(
                            f'Table "{table_name}" does not exist in database "{database}" on host "{host}"')
                    cols = await get_columns2(table_name=table_name, conn_obj=conn)
                    col_names = list()
                    upsert_col_fixes = list()
                    for c in cols:
                        col_name = c["column_name"]
                        col_names.append(col_name)
                        upsert_col_fixes.append(
                            f'{col_name}=EXCLUDED.{col_name}')
                    async with conn.transaction():
                        sql = f'INSERT INTO {table_name}({", ".join(col_names)})' \
                              f'\nVALUES ({", ".join([f"${i+1}" for i in  range(len(col_names))])})' \
                              f'\nON CONFLICT ON CONSTRAINT {table_name}_pkey DO UPDATE' \
                              f'\nSET {", ".join(upsert_col_fixes)}'

                        await conn.executemany(sql, records, timeout=timeout)
                        logger.debug(
                            f'{len(records)} records were written to database for channel {records[0][5]}')

    except socket.gaierror as se:
        host = conn_dict['host']
        logger.error(f'Host {host} is not reachable at this time')
        if se.errno == -2:
            e = Exception(f'Host {host} is not reachable')
            raise e
        else:
            raise se
    except Exception as e:
        raise


async def list_schemas(conn_obj=None, **conn_dict):
    """
    List all available schemas using either the conn-obj or conn_string
    :param conn_obj: an instance of asyncpg.Connection
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :return: the available schemas
    NB the information_schema and pg_catalog are excluded
    """
    qtext = f'SELECT schema_name FROM information_schema.schemata'
    if conn_obj is not None:
        res = await conn_obj.fetch(qtext)
    else:
        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT,  **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                async with conn_obj.transaction():
                    res = await conn_obj.fetch(qtext)

    return tuple(set([e['schema_name'] for e in res]) - set(['information_schema', 'pg_catalog']))


async def list_tables(conn_obj=None, schema=None, **conn_dict):
    """
    List the tables for a given database in a postgres server
    :param conn_obj: and instance of asyncpg.Connection
    :param conn_dict: dict whose keys are used top connect to a posgtres database using asyncpg
    :param schema, str, schema name, list only tables in the supplied schema
    :return: a tuple of strings representing the names of the tables that exists in the given database
    """

    if schema is not None:
        logger.debug(f'Listing tables from schema {schema}')
        qtext = f"SELECT * FROM pg_catalog.pg_tables WHERE schemaname = '{schema}';"

    else:
        logger.debug(f'Listing tables from all data schemas')
        qtext = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"

    if conn_obj is not None:
        if schema:
            available_schemas = await list_schemas(conn_obj=conn_obj)
            assert schema in available_schemas, f'schema "{schema}" does not exist in {conn_obj} '
        res = await conn_obj.fetch(qtext)
    else:

        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                if schema:
                    available_schemas = await list_schemas(conn_obj=conn_obj)
                    assert schema in available_schemas, f'schema "{schema}" does not exist in {conn_obj} '
                async with conn_obj.transaction():
                    res = await conn_obj.fetch(qtext)

    table_names = [f'{e["schemaname"]}.{e["tablename"]}' for e in res]
    return table_names


async def get_bbox(table_name=None, geom_column=None, srid=None, conn_obj=None, **conn_dict):
    """
    Conputer the spatial extent of a postgis layer in LAT/LON projection
    :param table_name: str, able name
    :param geom_column: str, the name of the geom column used to compute the extent
    :param srid: int, the srid code of the geom_column
    :param conn_obj: instance of asyncpg.Connection
    :param conn_dict: dict with connection info
    :return: the spatial extent (xmin, ymin, xmax, xmax) in LAT/LON, list
    """
    assert '.' in table_name, f'Invalid table_name={table_name}'
    schema, tname = table_name.split('.')
    VAC = True
    if VAC:
        logger.debug(f'using estimated extent')
        extq = f"ST_EstimatedExtent('{schema}', '{tname}', '{geom_column}')"
    else:
        extq = f'ST_Extent({geom_column})'

    if srid is None or srid == 4326:

        qtext = f'''
                    WITH bbox AS (
                        SELECT {extq} as extent
                        FROM {table_name}
                    )
                    SELECT
                        ST_Xmin(bbox.extent) as xmin,
                        ST_Ymin(bbox.extent) as ymin,
                        ST_Xmax(bbox.extent) as xmax,
                        ST_Ymax(bbox.extent) as ymax
                    FROM bbox;
            '''
    else:
        qtext = f'''
                            WITH bbox AS (
                                SELECT ST_Transform(ST_SetSRID({extq}, {srid}), 4326) as extent
                                FROM {table_name}
                            )
                            SELECT
                                ST_Xmin(bbox.extent) as xmin,
                                ST_Ymin(bbox.extent) as ymin,
                                ST_Xmax(bbox.extent) as xmax,
                                ST_Ymax(bbox.extent) as ymax
                            FROM bbox;
                    '''

    logger.debug(qtext)
    if conn_obj:
        resl = await conn_obj.fetch(qtext)
    else:
        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                async with conn_obj.transaction():
                    resl = await conn_obj.fetch(qtext)
    res = resl[0]

    return list((res['xmin'], res['ymin'], res['xmax'], res['ymax']))


async def get_table_cfg2(conn_obj=None, table_name=None, prop_filter_prefix='pub', **conn_dict):
    """
    Create a config dictionary suitable for configurring martin vector tile service
     for a given table located  in  a database defined by conn_obj or conn_dict
    :param conn_obj: instance of asyncpg.Connection,
    :param table_name: str, the name of the table
    :param prop_filter_prefix, str, the prefix used to filter the columns that will be published as properties of features
            in the vector tiles
    :param conn_dict: dict containing parameters to connect to DB
    :return: dict with the configuration as per https://github.com/urbica/martin#configuration-file
    NB: martin support tables/layers with more than one geometry column. This is desirabvle because
    the web mercator projectin EPSG 3857 should be used for displaying and is not suitable for
    performing spatial computations
    """
    assert '.' in table_name, f'Invalid table_name={table_name}'
    schema, tname = table_name.split('.')

    logger.info(f'Creating configuration for {table_name}')
    if conn_obj:
        columns = await get_columns2(table_name=table_name, conn_obj=conn_obj)
        geom_columns = [e for e in columns if 'geometry' in e['data_type']]
        print(geom_columns, table_name)
        attr_columns = [e for e in columns if 'geometry' not in e['data_type']]
        if len(geom_columns) == 0:
            logger.info(
                f'Skipping table {table_name}. No geometry columns detected')
            return

        if len(geom_columns) == 1:
            geom_column = geom_columns[0]['column_name']
            try:
                table_srid = int(
                    geom_columns[0]['data_type'][1:-1].split(',')[-1])
            except Exception:
                logger.error(
                    f'Skipping table {table_name}. Could not extract srid from geometry column "{geom_column}"')
                return
        else:
            # prefer 3857
            expr = ['3857' in e['data_type'] for e in geom_columns]
            if any(expr):
                geom_column = geom_columns[expr.index(True)]['column_name']
                try:
                    table_srid = int(geom_columns[expr.index(True)]
                                     ['data_type'][1:-1].split(',')[-1])
                except Exception:
                    logger.error(
                        f'Could not extract srid from geometry column "{geom_column}"')
                    return

            else:
                expr = ['4326' in e['data_type'] for e in geom_columns]

                if any(expr):
                    geom_column = geom_columns[expr.index(True)]['column_name']
                    table_srid = int(geom_columns[expr.index(True)]
                                     ['data_type'][1:-1].split(',')[-1])
                else:
                    # decide what to do, usually get the first available is the best option
                    geom_column = geom_columns[0]['column_name']
                    table_srid = int(
                        geom_columns[0]['data_type'][1:-1].split(',')[-1])

        rd = dict(id=table_name, schema=schema, table=tname)

        try:
            bb = await get_bbox(table_name=table_name, geom_column=geom_column, conn_obj=conn_obj)
        except Exception as e:
            logger.error(
                f'Failed to fetch bounding box for {table_name} because {e}. Skiping...')
            return

        rd['bounds'] = bb
        rd['srid'] = table_srid
        rd['geometry_column'] = geom_column
        rd['id_column'] = None

        rd['extent'] = 4096
        rd['buffer'] = 64
        rd['geometry_type'] = 'GEOMETRY'
        rd['clip_geometry'] = True
        props = dict()


        for c in attr_columns:
            if prop_filter_prefix is not None:
                if not c['column_name'].startswith(prop_filter_prefix):
                    continue
            props[c['column_name']] = c['type_name']
        rd['properties'] = props

        # return {table_name: rd}

    else:

        async with asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                       command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict) as pool:
            async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
                columns = await get_columns2(table_name=table_name, conn_obj=conn_obj)
                geom_columns = [
                    e for e in columns if 'geometry' in e['data_type']]
                attr_columns = [
                    e for e in columns if 'geometry' not in e['data_type']]
                if len(geom_columns) == 0:
                    logger.info(
                        f'Skipping table {table_name}. No geometry columns detected')
                    return
                if len(geom_columns) == 1:
                    geom_column = geom_columns[0]['column_name']
                    table_srid = int(
                        geom_columns[0]['data_type'][1:-1].split(',')[-1])
                else:
                    # prefer 3857
                    expr = ['3857' in e['data_type'] for e in geom_columns]

                    if any(expr):
                        geom_column = geom_columns[expr.index(
                            True)]['column_name']
                        table_srid = int(geom_columns[expr.index(True)]
                                         ['data_type'][1:-1].split(',')[-1])

                    else:
                        expr = ['4326' in e['data_type'] for e in geom_columns]
                        if any(expr):
                            geom_column = geom_columns[expr.index(
                                True)]['column_name']
                            table_srid = int(geom_columns[expr.index(
                                True)]['data_type'][1:-1].split(',')[-1])
                        else:
                            # decide what to do, ususally get the first available is the best option
                            geom_column = geom_columns[0]['column_name']
                            table_srid = int(
                                geom_columns[0]['data_type'][1:-1].split(',')[-1])

                rd = dict(id=table_name, schema=schema, table=tname)

                bb = await get_bbox(table_name=table_name, geom_column=geom_column, conn_obj=conn_obj)
                rd['bounds'] = bb
                rd['srid'] = table_srid
                rd['geometry_column'] = geom_column
                rd['id_column'] = None

                rd['extent'] = 4096
                rd['buffer'] = 64
                rd['geometry_type'] = 'GEOMETRY'
                rd['clip_geometry'] = True
                props = dict()


                for c in attr_columns:
                    if prop_filter_prefix is not None:
                        if not c['column_name'].startswith(prop_filter_prefix):
                            continue
                    props[c['column_name']] = c['type_name']
                rd['properties'] = props

    return {f'{table_name}:': rd}


# @atimeit
async def get_table_cfg(conn_obj=None, table_name=None,  **conn_dict):
    """
    Create a config dictionary suitable for configuring martin vector tile service
     for a given table located  in  a database defined by conn_obj or conn_dict
    :param conn_obj: instance of asyncpg.Connection,
    :param table_name: str, the name of the table
    :param conn_dict: dict containing parameters to connect to DB
    :return: dict with the configuration as per https://github.com/urbica/martin#configuration-file
    NB: martin support tables/layers with more than one geometry column. This is desirabvle because
    the web mercator projectin EPSG 3857 should be used for displaying and is not suitable for
    performing spatial computations
    """
    assert '.' in table_name, f'Invalid table_name={table_name}. Needs to be fully qualified: schema.table_name'
    schema, tname = table_name.split('.')

    logger.info(f'Creating configuration for {table_name}')

    if conn_obj is None:
        pool = await asyncpg.create_pool(min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                         command_timeout=POOL_COMMAND_TIMEOUT, **conn_dict)

        conn_obj = await pool.acquire(timeout=CONNECTION_TIMEOUT)

    columns = await get_columns(table_name=table_name, conn_obj=conn_obj)
    if not columns:
        logger.info(
            f'Skipping table {table_name}. No geometry columns detected')
        return
    srid = None
    for r in columns:  # prefer/pick the 3857 geom or the last in case 3857 is not used and more than 1
        srid = r['srid']
        geom_column = r['geom_column']
        geom_type = r['type']
        properties = r['properties']
        if srid == 3857:
            break
    tbl_dict = dict(id=table_name, schema=schema, table=tname)

    try:
        bb = await get_bbox(table_name=table_name, geom_column=geom_column, srid=srid, conn_obj=conn_obj)
    except Exception as e:
        logger.error(
            f'Failed to fetch bounding box for {table_name} because {e}. Skiping...')
        return

    tbl_dict['bounds'] = bb
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
        col_is_publishable = await column_is_publishable(table_name=table_name, column_name=k, conn_obj=conn_obj)
        if col_is_publishable is False:
            logger.debug(
                f'Column {k} from {table_name} is not publishable and will not be included in the config')
            continue
        props[k] = v

    tbl_dict['properties:'] = props

    if 'pool' in locals():
        await conn_obj.close()
        await pool.close()

    return {f'{table_name}:': tbl_dict}


def fetch_conn_string(from_file='./.env', ssl_require=True):
    """
    reads the .env into a dict
    :param from_file:
    :return:
    """
    rd = dict()
    with open(from_file) as src:
        for line in src:
            long_key, value = line.strip().split('=')
            key = long_key.split('_')[-1].lower()
            rd[key] = value
    if ssl_require:
        rd['sslmode'] = 'require'
    return rd


def read_conf(path=None):
    with open(path) as f:
        return yaml.full_load(f)


def dump(input_dict, depth=0, content=[]):
    """
    Recursively dumps a dictionary with the configuration into YAML format
    :param input_dict: dict, input
    :param depth: int, default=0, the depth of each line/elemnt in the dictionary. Use to insert \t characters
    :param content: the content created/passed in at every recursion
    :return:
    """
    spacer = ''.join(depth * [' '])
    for k, v in input_dict.items():
        if isinstance(v, dict):
            content.append(f'{spacer}{k}')
            dump(v, depth=depth + 2, content=content)
        else:
            content.append(
                f'{spacer}{k}: {str(v).lower() if type(v) is bool else v}')
    return '\n'.join(content)


async def create_config_dict(dsn=None, schemas=None):
    """
    Create a configuration dictionary for all table sources in a postgis database.
    If schema is provided the config is generated for the given schema
    :param schemas: iter of strings representing schemas in db
    :return: a dictionary with configuration for every layer
    """
    schemas_cfg = {}
    if schemas:
        logger.info(
            f'Creating config dict for tables in \"{", ".join(schemas)}" schemas ...')
    else:
        logger.info(
            f'Creating config dict for tables in all available schemas ...')
    async with asyncpg.create_pool(dsn=dsn, min_size=POOL_MINSIZE, max_size=POOL_MAXSIZE,
                                   command_timeout=POOL_COMMAND_TIMEOUT,) as pool:
        logger.debug('Connecting to database...')
        async with pool.acquire(timeout=CONNECTION_TIMEOUT) as conn_obj:
            available_schemas = await list_schemas(conn_obj=conn_obj)
            available_schemas = set(available_schemas) - set(['public'])

            for schema in schemas:

                logger.debug(f'Checking if schema {schema} exists')
                if not schema in available_schemas:
                    logger.warning(f'Schema "{schema}" does not exist in {dsn}.'
                                   f'Valid options are: {",".join(available_schemas)}')
                else:
                    for table_name in await list_tables(conn_obj=conn_obj, schema=schema):
                        try:
                            will_publish = await table_is_publishable(table_name=table_name, conn_obj=conn_obj)
                        except Exception as ee:
                            logger.error(
                                f'Failed to fetch comments for table {table_name} because {ee}. Skipping...')
                            continue
                        if will_publish == False:
                            logger.info(
                                f'{table_name} was marked as not publishable and will be skipped')
                            continue
                        table_cfg = await get_table_cfg(conn_obj=conn_obj, table_name=table_name)
                        if table_cfg:
                            schemas_cfg.update(table_cfg)

            # funcs_cfg = {}
            # func_list = (cwd / '../functions').glob('*.sql')
            # for func_file in func_list:
            #     assert '.' in func_file.stem, f'Invalid function_name={func_file.name}. Needs to be fully qualified: ' \
            #                                   f'schema.function_name '
            #     funcs_cfg[func_file.stem + ':'] = {
            #         'id': func_file.stem,
            #         'schema': func_file.stem.split('.')[0],
            #         'function': func_file.stem.split('.')[1],
            #         'minzoom': 0,
            #         'maxzoom': 30,
            #         'bounds': [-180.0, -90.0, 180.0, 90.0],
            #     }

    return {'table_sources:': schemas_cfg}#, 'function_sources:': funcs_cfg}


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
def main():
    import os
    logging.basicConfig()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s', "%Y-%m-%d %H:%M:%S"))

    # remove the default stream handler and add the new on too it.
    logger.handlers.clear()
    logger.addHandler(sthandler)

    logger.setLevel('INFO')
    logger.name = os.path.split(__file__)[-1]

    import argparse as ap

    class HelpParser(ap.ArgumentParser):
        def error(self, message):

            self.print_help()
            exit(0)

    arg_parser = HelpParser(formatter_class=ap.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument('-dsn', '--postgres-dsn-string', type=str, default=None,
                            help='Connection string to Postgres server', )

    arg_parser.add_argument('-s', '--database-schemas', default='public',
                            help='A list of schema names',
                            type=str, nargs='+', )
    # arg_parser.add_argument('-igc', '--include_general_config',
    #                         help='flag to include the general config or no', type=bool, default=True
    #                         )
    arg_parser.add_argument('-o', '--out-cfg-file',
                            help='Full path to the config file to be created. If not supplied the YAML fill be dumped to'
                                 'stdout ', type=str,
                            default=None)

    arg_parser.add_argument('-ufs', '--upload-to-file-share',
                            help='The name of the Azure file share where the config will be uploaded', type=str,
                            default=None,required=False)

    arg_parser.add_argument('-surl', '--sas_url',
                            help='A full SAS URL of the Azure file share where the config will be uploaded', type=str,
                            default=None, required=False)

    #parse and collect args
    args = arg_parser.parse_args()
    dsn = args.postgres_dsn_string or os.environ.get('DATABASE_CONNECTION', None)
    schemas = args.database_schemas
    out_yaml_file = args.out_yaml_file
    azure_file_share_name = args.upload_to_file_share or os.environ.get('AZURE_FILESHARE_NAME', None)
    sas_url=args.sas_url or os.environ.get('AZURE_FILESHARE_SASURL', None)
    azure_storage_account=args.azure_storage_account or os.environ.get('AZURE_STORAGE_ACCOUNT', None)

    # transform schemas into iter
    schemas = set(schemas)


    # execute
    if not dsn:
        raise Exception(f'PostGIS database connection string is required through either -dsn option or '
                        f'DATABASE_CONNECTION of environmental variable.')

    config = create_general_config()

    schemas_cfg = asyncio.run(create_config_dict(
            dsn=dsn,
            schemas=schemas,
         ))

    config.update(schemas_cfg)

    yaml_config = dump(config)
    logger.info(f'Writing config to {out_yaml_file}')
    with open(out_yaml_file, 'w+') as f:
        f.write(yaml_config)
        #yaml.safe_dump(config, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

    if not (sas_url and azure_file_share_name and azure_storage_account): # there is a default share mconfig
        logger.info(f'Could not connect to Azure Storage.'
                    f'It requires to configure a SAS URL, a file share name and a Azure Storage Account Name through either CLI options or environmental variables.')
        exit(0)

    from martin_config.azfile import upload_cfg_file
    upload_cfg_file(
        azure_storage_account=azure_storage_account,
        sas_url=sas_url,
        share_name=azure_file_share_name,
        cfg_file_path=out_yaml_file
    )
