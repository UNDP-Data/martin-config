# martin-config

A python tool to generate config.yaml for [martin](https://github.com/maplibre/martin) vector tiles server
from PostGIS database leveraging asyncpg
library. <br/> 
martin server uses a YAML config file to specify what tables/functions from the PostGIS DB
will be published. 


- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)

##Features

---

martin-config uses asyncpg to interact with the Postgres server and supports following functionality:
- [x] creating config for table sources
- [x] creating config for function sources
- [x] creating config for  the general section
- [x] operate in one, multiple or all schemas
- [x] selective publishing of tables through table comments
- [x] selective publishing of columns through column comments
- [x] uploading the config file to an Azure File Share using  SAS authentication

Its strongest feature is the ability to select only the mark tables and table columns.
This is achieved by inserting the 


```publish=True/False```.

into the comment of a given table/column. The folowing rules apply
- if a table does not have this string in the comments it is considered unmarked and unpublishable
- if a has this string in comments its value dictates if the table is publishable (True) or unpublishable (False)
- same logic applied to columns with the difference that unmarked columns are **publishable by default**

This allows users to mark/set the tables/columns selectively and use martin-config  to generate 
a conforming config file.

## Requirements

---

asyncpg, yaml, dotenv and optionally azure-file-share

## Install

---

1. Create a pipenv based venv
```bash
python3 -m pip install pipenv
pipenv --python 3
```
2. install martin-config
```commandline
pipenv run pip install martin-config
```
3. optionally install azure functionality to be able to upload the cfg to AFS
```commandline
pipenv run pip install martin-config[azure]
```

## Usage

---

- prepare

```bash
pipenv shell
```

- help

```bash
python src/martin_config -h
usage: martin_config [-h] [-dsn POSTGRES_DSN_STRING] [-pfp PROP_FILTER_PREFIX] [-s DATABASE_SCHEMA]
                     [-igc INCLUDE_GENERAL_CONFIG] [-o OUT_YAML_FILE] [-ufs UPLOAD_TO_FILE_SHARE] [-surl SAS_URL]
                     [-asa AZURE_STORAGE_ACCOUNT]

optional arguments:
  -h, --help            show this help message and exit
  -dsn POSTGRES_DSN_STRING, --postgres_dsn_string POSTGRES_DSN_STRING
                        Connection string to Postgres server (default: None)
  -pfp PROP_FILTER_PREFIX, --prop_filter_prefix PROP_FILTER_PREFIX
                        S tring to filter column for every table. Column that start with this string will be added to the
                        configuration (default: None)
  -s DATABASE_SCHEMA, --database_schema DATABASE_SCHEMA
                        The name of the schema to generate the config for (default: None)
  -igc INCLUDE_GENERAL_CONFIG, --include_general_config INCLUDE_GENERAL_CONFIG
                        falg to include the general config or no (default: True)
  -o OUT_YAML_FILE, --out_yaml_file OUT_YAML_FILE
                        Full path to the config file to be created. If not supplied the YAML fill be dumped tostdout (default:
                        None)
  -ufs UPLOAD_TO_FILE_SHARE, --upload_to_file_share UPLOAD_TO_FILE_SHARE
                        The name of the Azure file share where the config will be uploaded (default: None)
  -surl SAS_URL, --sas_url SAS_URL
                        A full SAS URL of the Azure file share where the config will be uploaded (default: None)
  -asa AZURE_STORAGE_ACCOUNT, --azure_storage_account AZURE_STORAGE_ACCOUNT
                        The name of Azure Storage Account (default: None)
```

- run

```bash
# create .env to edit your PostGIS connection string and Azure Storage connection information
cp .env.example .env

python ./src/martin_config -o ./config.yaml

# instead of creating .env, you can also specify envrionmental variables directly before the command
DATABASE_CONNECTION=$DATABASE_CONNECTION\
AZURE_STORAGE_ACCOUNT=$AZURE_STORAGE_ACCOUNT\
AZURE_FILESHARE_SASURL=$AZURE_FILESHARE_SASURL\
AZURE_FILESHARE_NAME=$AZURE_FILESHARE_NAME\
python ./src/martin_config -o ./config.yaml

# Also, you can determine these parameters through the command options
python ./src/martin_config -o ./config.yaml -dsn $DATABASE_CONNECTION -ufs $AZURE_FILESHARE_NAME -surl $AZURE_FILESHARE_SASURL -asa $AZURE_STORAGE_ACCOUNT

# for exporting specific scheme only
python ./src/martin_config -s global -o ./config.yaml
```

if `-s global` is not specified, it will generate for all schemes.

If you want to export multiple schemes, you can specilify like `-s global zambia`.

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
root@5311a6453ce3:/home/undp/src# martin_config -h
```
