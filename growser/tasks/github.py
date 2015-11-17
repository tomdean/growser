import gzip
import json
import time
import os

from growser.app import app
from growser.services import github
from growser.models import Repository


def get_rate_limit():
    rate = json.loads(github.rate_limit().decode('UTF-8'))['resources']
    if rate['search']['remaining'] == 0:
        return rate['search']['reset']
    return rate['core']['reset']


def get_with_retry(user: str, name: str):
    rv = github.repository(user, name)
    if 'rate limit exceeded' in rv:
        reset = get_rate_limit() - int(time.time())
        app.logger.info("Sleeping for {}".format(reset))
        time.sleep(reset)
        rv = get_with_retry(user, name)
    return rv


def update_repository_data():
    top = Repository.query.order_by(Repository.num_unique.desc()).limit(25000)
    for idx, repo in enumerate(top.all()):
        filename = "data/github-api/{}.json.gz".format(repo.repo_id)
        if os.path.exists(filename):
            last_checked = (time.time() - os.path.getmtime(filename)) / (60*60)
            if last_checked < 48:
                continue
        app.logger.debug("Fetching {} ({})".format(repo.name, idx))
        content = github.repository(*repo.name.split("/"))
        with gzip.open(filename, "wb") as gz:
            gz.write(content)
