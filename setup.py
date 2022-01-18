from distutils.core import setup
from os import path
from io import open

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='martin-config',
    version='0.0.1',
    description='This package is to generate config.yaml for martin from PostGIS database',
    long_description=long_description, 
    long_description_content_type="text/markdown",
    author="Ioan Ferencik", 
    author_email="ioan.ferencik@undp.org",
    url="https://github.com/UNDP-Data/martin-config",
    packages=['martin-config',],
    keywords="martin postgis yaml config",
    package_dir = {'': 'src'},
    python_requires=">=3.6, <4",
    install_requires=["asyncio==3.4.3", "asyncpg==0.25.0", "pyyaml==6.0"],
)
