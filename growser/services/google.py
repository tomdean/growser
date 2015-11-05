from apiclient.discovery import build
from apiclient.errors import HttpError
from httplib2 import Http
from oauth2client.client import SignedJwtAssertionCredentials


def bigquery(account_name, private_key):
    return __client('bigquery', 'v2', account_name, private_key,
                    'https://www.googleapis.com/auth/bigquery')


def storage(account_name, private_key):
    return __client('storage', 'v1', account_name, private_key,
                    'https://www.googleapis.com/auth/devstorage.full_control')


def __client(service_name, version, account_name, private_key, scope):
    auth = SignedJwtAssertionCredentials(account_name, private_key, scope)
    return build(service_name, version, http=auth.authorize(Http()))


HttpError = HttpError
