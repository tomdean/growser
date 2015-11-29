import datetime
import gzip
import json
import time
import os

from sqlalchemy import func

from growser.app import app
from growser.services import github
from growser.models import db, Rating, Repository


def get_rate_limit():
    """Return the current rate limit from the API."""
    rate = json.loads(github.rate_limit().decode('UTF-8'))['resources']
    if rate['search']['remaining'] == 0:
        return rate['search']['reset']
    return rate['core']['reset']


def get_with_retry(user: str, name: str) -> str:
    """Wrapper for API requests to GitHub API with rate limit support."""
    rv = github.repository(user, name)
    if b'rate limit' in rv:
        reset = get_rate_limit() - int(time.time())
        app.logger.info("Sleeping for {}".format(reset))
        time.sleep(reset)
        rv = get_with_retry(user, name)
    return rv


def update_repositories(repos: list, age: int=86400*14):
    """Call the API for the given repositories, saving the results to disk.

    Defaults to caching requests for 14 days."""
    for idx, repo in enumerate(repos):
        filename = "data/github-api/{}.json.gz".format(repo.repo_id)
        if os.path.exists(filename):
            stats = os.path.getctime(filename)
            if age > time.time() - stats:
                continue
        app.logger.debug("Fetching {} ({})".format(repo.name, idx))
        content = get_with_retry(*repo.name.split("/"))
        with gzip.open(filename, "wb") as gz:
            gz.write(content)


def update_popular_repositories(limit: int=25000):
    """Update local data for repositories sorted by most stars & forks."""
    top = Repository.query.order_by(Repository.num_unique.desc()).limit(limit)
    update_repositories(top.all())


def update_recently_popular_repositories(prior_days=7, limit: int=500):
    """Update local data for most recently popular repositories."""
    count = func.COUNT(Rating.repo_id)
    created_at = datetime.date.today() - datetime.timedelta(days=prior_days)
    top = db.session.query(Rating.repo_id, count.label("num_events")) \
        .filter(Rating.created_at >= created_at) \
        .group_by(Rating.repo_id) \
        .order_by(count.desc()) \
        .limit(limit)
    update_repositories(top.all())
