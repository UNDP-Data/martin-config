import os

from azure.storage.fileshare import  ShareServiceClient



def upload_cfg_file(
        azure_storage_account='undpngddlsgeohubdev01',
        sas_url=None,
        share_name='mconfig',
        cfg_file_path=None,
        file_name=None
):
    """
    Upload the config file to an Azure file share
    :param azure_storage_account: str, the storage account name, default=undpdev..
    :param sas_url: str, the file share  SAS URL of the storage account
            used to authenticate te requests and needs to have RWLC rights
    :param share_name: str, the name of the share where the cfg will be uploaded
    :param cfg_file_path: str, abs path to a file to be uploaded
    :param file_name:
    :return:

    If sas_url is not provided the AZURE_FILESHARE_SASURL variable can be used to supply a
    valid authentication SAS URL
    """



    url = sas_url or os.environ.get(['AZURE_FILESHARE_SASURL'], None)
    if url in ('', None):
        raise Exception(f'Could not get a SAS  from sas_url arg or AZURE_FILESHARE_SASURL env variable')

    assert azure_storage_account in url, f'Invalid url={url}'

    assert cfg_file_path not in ('', None), f'Invalid config_file_path={cfg_file_path}'
    assert os.path.exists(cfg_file_path), f'cfg_file_path does not exist on local file system'

    if file_name is None:
        file_name = os.path.split(cfg_file_path)[-1]

    with ShareServiceClient(sas_url) as ssc:

        shares = [e.name for e in ssc.list_shares()]
        assert share_name in shares, f'share_name={share_name} does not exists under account {azure_storage_account}\n' \
                                     f'Valid option are {",".join(shares)}'

        with ssc.get_share_client(share_name) as sc:
            with sc.get_directory_client() as sdc:
                with open(cfg_file_path, 'rb') as cfgf:
                    sdc.upload_file(file_name=file_name, data=cfgf.read())


