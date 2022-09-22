import os
from urllib.parse import urlparse
from azure.storage.fileshare import ShareDirectoryClient
import logging

logger = logging.getLogger(__name__)

def upload_cfg_file(
        sas_url=None,
        cfg_file_path=None,
        file_name=None
):
    """
    Upload the config file to an Azure file share
    :param sas_url: str, the file share  SAS URL of the storage account
            used to authenticate te requests and needs to have RWLC rights
    :param cfg_file_path: str, abs path to a file to be uploaded
    :param file_name:the name of the destination file
    :return:

    """

    parsed = urlparse(sas_url)
    assert cfg_file_path not in ('', None), f'Invalid config_file_path={cfg_file_path}'
    assert os.path.exists(cfg_file_path), f'cfg_file_path does not exist on local file system'

    if file_name is None:
        file_name = os.path.split(cfg_file_path)[-1]

    with ShareDirectoryClient.from_directory_url(directory_url=sas_url) as sdc:
        with open(cfg_file_path, 'rb') as src:
            sdc.upload_file(file_name=file_name, data=src)
            logger.info(f'{file_name} was uploaded to {parsed.scheme}//{parsed.netloc}{parsed.path}')




