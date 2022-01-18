# martin-config

This repository is to manage the python script to generate config.yaml for martin from PostGIS database.

## Installation

```bash
pip install pipenv
pipenv install
```

## Run the script

- prepare

```bash
pipenv shell
```

- help

```bash
python src/config.py -h
usage: config.py [-h] -dsn POSTGRES_DSN_STRING [-pfp PROP_FILTER_PREFIX] [-s DATABASE_SCHEMA] [-igc INCLUDE_GENERAL_CONFIG] [-o OUT_YAML_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -dsn POSTGRES_DSN_STRING, --postgres_dsn_string POSTGRES_DSN_STRING
                        Connection string to Postgres server (default: None)
  -pfp PROP_FILTER_PREFIX, --prop_filter_prefix PROP_FILTER_PREFIX
                        S tring to filter column for every table. Column that start with this string will be added to the configuration (default: None)
  -s DATABASE_SCHEMA, --database_schema DATABASE_SCHEMA
                        The name of the schema to generate the config for (default: None)
  -igc INCLUDE_GENERAL_CONFIG, --include_general_config INCLUDE_GENERAL_CONFIG
                        falg to include the general config or no (default: True)
  -o OUT_YAML_FILE, --out_yaml_file OUT_YAML_FILE
                        Full path to the config file to be created. If not supplied the YAML fill be dumped tostdout (default: None)
```

- run

```bash
DATABASE_CONNECTION="postgres://geohubusr:W%40s%243qklin@undp-ngd-psql-gishub-01-dev.postgres.database.azure.com:5432/geodata?sslmode=require"
python ./src/martin-config -dsn="$DATABASE_CONNECTION" -o ./config.yaml

# for exporting specific scheme only
python ./src/martin-config -dsn="$DATABASE_CONNECTION" -s global -o ./config.yaml
```

if `-s global` is not specified, it will generate for all schemes.

For improving the performance of the script, please create statistics for all tables by the following SQL.

```sql
-- analyze all tables
ANALYZE;

-- analyze specific table
ANALYZE electricity.access2012;
```

## install by setup.py

```bash
python setup.py install
```

for uninstalling,

```bash
pip uninstall martin-config
```

## Docker

```bash
$docker build -t martin-config:latest .
$docker run -ti --rm -v $(pwd):/home/undp/src martin-config:latest

root@0e89ee1bded9:/home/undp/src# azcopy -v
azcopy version 10.13.0
```
