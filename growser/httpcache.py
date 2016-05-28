import hashlib
import gzip
import os
import time
from urllib.parse import urlparse

import requests


def get(url, params=None, expiry=86400, **kwargs):
    """Wrapper around requests.get"""
    path = _url_to_path(url, params)
    if os.path.exists(path) and expiry > time.time() - os.path.getctime(path):
        return gzip.open(path, 'rb').read()

    name = os.path.dirname(path)
    if not os.path.exists(name):
        os.makedirs(name)

    content = requests.get(url, params, **kwargs).content
    gzip.open(path, 'wb').write(content)

    return content


def _url_to_path(url, params=None):
    """Returns a filesystem-friendly string from a URL."""
    parts = urlparse(url)
    domain = parts.netloc
    if domain[0:4] == 'www.':
        domain = domain[4:]
    filename = parts.path
    if filename[0] == '/':
        filename = filename[1:]
    if params:
        params_hash = repr(sorted(params.items())).encode('UTF-8')
        filename += "." + hashlib.sha1(params_hash).hexdigest()
    return os.path.join("data", "httpcache", domain, filename + ".gz")
