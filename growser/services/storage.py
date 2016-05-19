from io import FileIO
import os

from apiclient.http import MediaIoBaseDownload

from growser.services.google import BaseJob, HttpError, CloudStorageService


def configure(app):
    return CloudStorageService(
        app.config.get('GOOGLE_PROJECT_ID'),
        app.config.get('GOOGLE_CLIENT_KEY')
    )


class DownloadFile(BaseJob):
    """Download a file from a Google Cloud Storage bucket to a local path."""
    def run(self, bucket: str, obj: str, local_path: str):
        archive = self.api.objects.get_media(bucket=bucket, object=obj)
        filename = os.path.join(local_path, os.path.basename(obj))
        with FileIO(filename, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, archive, chunksize=1024*1024)
            complete = False
            while not complete:
                _, complete = downloader.next_chunk()
        return filename


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
    """Return a list of all files matching `prefix`."""
    def run(self, bucket: str, prefix: str):
        response = self.api.objects \
            .list(bucket=bucket, prefix=prefix).execute()
        return [i for i in response['items'] if int(i['size']) > 0]


class DownloadBucketPath(BaseJob):
    """Download a Google Storage bucket to a local path."""
    def run(self, bucket: str, bucket_path: str, local_path: str):
        archives = FindFilesMatchingPrefix(self.api).run(bucket, bucket_path)
        filenames = []
        for file in archives:
            filenames.append(DownloadFile(self.api).run(
                bucket, file['name'], local_path))
            DeleteFile(self.api).run(bucket, file['name'])
        return filenames
