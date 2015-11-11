from io import FileIO
import os

from apiclient.http import MediaIoBaseDownload

from growser.services.google import storage, HttpError


class CloudStorageService(object):
    def __init__(self, project_id, account_name, private_key):
        self.project_id = project_id
        self.api = storage(account_name, private_key)

    @property
    def objects(self):
        return self.api.objects()

    @property
    def buckets(self):
        return self.api.buckets()


class BaseJob(object):
    def __init__(self, api):
        self.api = api


class DownloadFile(BaseJob):
    def run(self, bucket: str, obj: str, destination_path: str):
        archive = self.api.objects.get_media(bucket=bucket, object=obj)
        filename = os.path.join(destination_path, os.path.basename(obj))
        with FileIO(filename, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, archive, chunksize=1024*1024)
            complete = False
            while not complete:
                _, complete = downloader.next_chunk()


class DeleteFile(BaseJob):
    def run(self, bucket: str, obj: str):
        try:
            self.api.objects.delete(bucket=bucket, object=obj).execute()
            return True
        except HttpError:
            return False


class FindFilesMatchingPrefix(BaseJob):
    def run(self, bucket: str, prefix: str):
        response = self.api.objects \
            .list(bucket=bucket, prefix=prefix).execute()
        return [i for i in response['items'] if 'gz' in i['name']]
