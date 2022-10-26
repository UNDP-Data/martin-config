import os.path
import yaml
from pathlib import Path
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse
from urllib.parse import quote_plus, urlencode



CWD = Path(__file__).parent
POOL_COMMAND_TIMEOUT = 15 * 60  # seconds
POOL_MINSIZE = 3
POOL_MAXSIZE = 5
CONNECTION_TIMEOUT = 30

def get_sqlfile_content(sql_file_name=None):
    """
    Reda the content of a SQL file from sql folder
    :param sql_file_name:
    :return: str, the content of the SQL file, as is
    """
    assert sql_file_name is not None, f'Invalid sql_file={sql_file_name}'
    sql_file_path = os.path.join(CWD, 'sql', sql_file_name)
    assert os.path.exists(sql_file_path), f'{sql_file_path} does not exist'
    with open(sql_file_path) as f:
        return f.read()


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
    if input_dict:
        for k, v in input_dict.items():
            if isinstance(v, dict):
                content.append(f'{spacer}{k}')
                dump(v, depth=depth + 2, content=content)
            else:
                content.append(
                    f'{spacer}{k}: {str(v).lower() if type(v) is bool else v}')
        return '\n'.join(content)
