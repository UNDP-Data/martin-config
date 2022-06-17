import os

database = os.environ.get('DATABASE_CONNECTION')

azure_account = os.environ.get('AZURE_STORAGE_ACCOUNT')
azure_container = os.environ.get('AZURE_FILESHARE_NAME')
azure_sas_url = os.environ.get('AZURE_FILESHARE_SASURL')
