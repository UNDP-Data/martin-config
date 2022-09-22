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
the installer creates a command line script **martincfg**

with pipenv it can be invoked like this:

```commandline
pipenv run martincfg
```


```bash
pipenv run martincfg
Loading .env environment variables...
usage: martincfg [-h] [-s DATABASE_SCHEMA [DATABASE_SCHEMA ...]] [-o OUT_CFG_FILE] [-e ENV_FILE] [-d] [-sfs]

Create a config file for martin vector tile server

optional arguments:
  -h, --help            show this help message and exit
  -s DATABASE_SCHEMA [DATABASE_SCHEMA ...], --database-schema DATABASE_SCHEMA [DATABASE_SCHEMA ...]
                        A list of schema names. If no schema is specified all schemas are used.
  -o OUT_CFG_FILE, --out-cfg-file OUT_CFG_FILE
                        Full path to the config file to be created. If not supplied the YAML fill be dumped to stdout
  -e ENV_FILE, --env-file ENV_FILE
                        Load environmental variables from .env file
  -d, --debug           Set log level to debug
  -sfs, --skip-function-sources
                        Do not create config for function sources

```


## Environmental variables
The arguments are self-explanatory. However, environmental variables
are also required to be available at runtime, specifically:

```bash
 POSTGRES_DSN=postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
 
 AZURE_FILESHARE_SASURL=https://[storageaccount].file.core.windows.net/[sharename]?[SAS_QUERY_STRING]
```
***Tips:*** <br/>
- If no schema is specified all schemas are used.
- multiple schemas can be specified one after another ``public myschema`` or ``public,myschema``
- .env file containing the above environmental variable can be used to the same effect as defining env. variables 

For improving the performance of the script, please create statistics for all tables by the following SQL.

```sql
-- analyze all tables
ANALYZE;

-- analyze specific table
ANALYZE schema.tablename;
```


```
