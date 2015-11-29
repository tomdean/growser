from io import FileIO
import os

from apiclient.http import MediaIoBaseDownload

from growser.services.google import BaseJob, HttpError


class DownloadFile(BaseJob):
    """Download a file from a Google Cloud Storage bucket to a local path."""
    def run(self, bucket: str, obj: str, destination_path: str):
        archive = self.api.objects.get_media(bucket=bucket, object=obj)
        filename = os.path.join(destination_path, os.path.basename(obj))
        with FileIO(filename, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, archive, chunksize=1024*1024)
            complete = False
            while not complete:
                _, complete = downloader.next_chunk()


class DeleteFile(BaseJob):
    """Delete a file from a Google Cloud Storage bucket"""
    def run(self, bucket: str, obj: str):
        try:
            self.api.objects.delete(bucket=bucket, object=obj).execute()
            return True
        except HttpError:
            # Error is returned if the object does not exist - can ignore
            return False


class FindFilesMatchingPrefix(BaseJob):
    """Return a list of all files matching"""
    def run(self, bucket: str, prefix: str):
        response = self.api.objects \
            .list(bucket=bucket, prefix=prefix).execute()
        return [i for i in response['items'] if 'gz' in i['name']]
