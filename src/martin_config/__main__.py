import os
import asyncio
import logging
import argparse
import sys
from martin_config import utils, config
from dotenv import dotenv_values



def main():
    logging.basicConfig()
    sthandler = logging.StreamHandler()
    sthandler.setFormatter(
        logging.Formatter('%(asctime)s-%(filename)s:%(funcName)s:%(lineno)d:%(levelname)s:%(message)s',
                          "%Y-%m-%d %H:%M:%S"))
    logger = logging.getLogger()
    # remove the default stream handler and add the new one too it.
    logger.handlers.clear()
    logger.addHandler(sthandler)
    # silence azure http logger
    azlogger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
    azlogger.setLevel(logging.WARNING)




    logger.name = os.path.split(__file__)[-1]

    parser = argparse.ArgumentParser(description='Create a config file for martin vector tile server')

    parser.add_argument('-s', '--database-schema',
                            help='A list of schema names. If no schema is specified all schemas are used.',
                            type=str, nargs='+', )
    parser.add_argument('-u', '--database-user',
                            help='The user for which the config will be created',
                            type=str, required=True )
    parser.add_argument('-o', '--out-cfg-file',
                            help='Full path to the config file to be created. If not supplied the YAML fill be dumped '
                                 'to stdout ', type=str,default=None)


    parser.add_argument('-e', '--env-file',
                        help='Load environmental variables from .env file', type=str,
                        default=None,required=False)
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Set log level to debug'
                        )
    parser.add_argument('-sfs', '--skip-function-sources',  action='store_true',
                        help='Do not create  config for function sources'
                        )

    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    schemas = args.database_schema
    if schemas:
        schemas = set(schemas[0].split(',') if ',' in schemas[0] else schemas)
    for_user = args.database_user
    config_file = args.out_cfg_file
    skip_function_sources = args.skip_function_sources
    debug = args.debug
    if debug:
        logger.debug('Setting log level to DEBUG')
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


    signed_azure_file_share_url = os.environ.get('AZURE_FILESHARE_SASURL', None)
    dsn = os.environ.get('POSTGRES_DSN', None)

    env_file = args.env_file

    if env_file:
        assert os.path.exists(env_file), f'.env file {env_file} does not exist'
        assert os.path.getsize(env_file) > 0, f'.env file {env_file} is empty'
        env_dict = dotenv_values(env_file)
        if signed_azure_file_share_url is None:
            signed_azure_file_share_url = env_dict.get('AZURE_FILESHARE_SASURL', None)
        if dsn is None:
            dsn = env_dict.get('POSTGRES_DSN', None)

    assert config_file not in [None, ''], f'config_file={config_file} is invalid'


    assert dsn is not None, f'Invalid POSTGRES_DSN={dsn}. Set env variable POSTGRES_DSN to a valid Postgres ' \
                            f'connection string.'

    general_config = config.create_general_config()

    schemas_config = asyncio.run(config.create_config_dict(
        dsn=dsn,
        for_user=for_user,
        schemas=schemas,
        skip_function_sources=skip_function_sources
    ))

    general_config.update(schemas_config or {})

    yaml_config = utils.dump(general_config)
    logger.info(f'Writing config to {config_file}')
    with open(config_file, 'w+') as f:
        f.write(yaml_config)


    if signed_azure_file_share_url:
        from martin_config.azfile import upload_cfg_file
        logger.info(f'Uploading cfg file to Azure File Share')
        upload_cfg_file(
            sas_url=signed_azure_file_share_url,
            cfg_file_path=config_file,
        )

if __name__ == '__main__':
    main()