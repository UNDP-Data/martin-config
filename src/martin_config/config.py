from martin_config import utils, db
import logging
import asyncpg
logger = logging.getLogger(__name__)
async def create_config_dict(dsn=None, schemas=None, for_user=None, skip_function_sources=False, **conn_dict):
    """
    Create a configuration dictionary for all table sources in a postgis database.
    If schema is provided the config is generated for the given schema
    :param dsn, str, a Postgres dsn  connection string
    :param schemas: iter of strings representing schemas in db
    :param skip_function_sources, bool, if True not config will be generated for the function sources
    :param conn_dict, dict, a dict with items containing  info  necessary to connect to a Postgres server
    :return: a dictionary with configuration for  tables sources and function sources
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
            f'Creating config for tables in all available schemas ...')
    funcs_cfg = {}
    async with asyncpg.create_pool(dsn=dsn, min_size=utils.POOL_MINSIZE, max_size=utils.POOL_MAXSIZE,
                                   command_timeout=utils.POOL_COMMAND_TIMEOUT, ) as pool:
        logger.debug('Connecting to database...')
        async with pool.acquire(timeout=utils.CONNECTION_TIMEOUT) as conn_obj:
            available_schemas = await db.list_schemas(conn_obj=conn_obj)
            #available_schemas = set(available_schemas) - set(['public'])
            if not schemas:
                schemas = available_schemas
            for schema in schemas:
                if for_user:
                    logger.debug(f'Checking if user {for_user} has usage privilege on {schema}')
                    if not await db.schema_is_accessible(conn_obj=conn_obj, schema=schema, user=for_user):
                        logger.info(f'User {for_user} has not been granted USAGE privilege on schema {schema}')
                        continue
                logger.debug(f'Checking if schema {schema} exists')
                if not schema in available_schemas:
                    logger.warning(f'Schema "{schema}" does not exist in {dsn}.'
                                   f'Valid options are: {",".join(available_schemas)}')
                    continue
                else:
                    for table in await db.list_tables(conn_obj=conn_obj, schema=schema):
                        try:
                            if for_user:
                                will_publish = await db.table_is_accessible(conn_obj=conn_obj, table=table, user=for_user)
                            else: # old mode, will be deprecated
                                will_publish = await db.table_is_publishable(table=table, conn_obj=conn_obj)
                        except Exception as ee:
                            logger.error(
                                f'Failed to fetch comments for table {table} because {ee}. Skipping...')
                            continue
                        if will_publish == False:
                            logger.info(
                                f'{table} was marked as not publishable and will be skipped')
                            continue
                        table_cfg = await db.get_table_cfg(conn_obj=conn_obj, user=for_user, table=table)
                        if table_cfg:
                            schemas_cfg.update(table_cfg)

                    if skip_function_sources is False:
                        funcs = await db.list_function_sources(conn_obj=conn_obj, schema=schema)
                        if funcs:
                            logger.info(f'Creating config for {len(funcs)} function source(s)...')
                            for func_rec in funcs:
                                func_name = f'{func_rec["function_schema"]}.{func_rec["function_name"]}'
                                if for_user:
                                    is_publishable = await db.function_is_publishable(
                                        conn_obj=conn_obj,user=for_user,function_name=func_rec['function_name'],
                                        schema=func_rec['function_schema']
                                    )
                                    if not is_publishable:
                                        logger.info(f'user {for_user} was not granted execute privilege for function {func_name}')
                                        continue
                                funcs_cfg[f'{func_name}:'] = {
                                    'id': func_name,
                                    'schema': func_rec['function_schema'],
                                    'function': func_rec['function_name'],
                                    'minzoom': 0,
                                    'maxzoom': 22,
                                    'bounds': [-180.0, -90.0, 180.0, 90.0],
                                }

    if schemas_cfg and funcs_cfg:

        return {'table_sources:': schemas_cfg, 'function_sources:': funcs_cfg }
    if schemas_cfg and not funcs_cfg:
        return  {'table_sources:': schemas_cfg}
    if funcs_cfg and not schemas_cfg:
        return  {'function_sources:': funcs_cfg}


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
