import datetime
import gzip
import json
import time
import os

from sqlalchemy import func

from growser.app import celery, db, log
from growser.services import github
from growser.models import Rating, Repository
from growser.tasks.screenshots import update_repository_screenshots


def update_repositories(repos: list, age: int=86400*14):
    """Call the API for the given repositories, saving the results to disk."""
    for idx, repo in enumerate(repos):
        update_repository.delay(repo, age)
        update_repository_screenshots.delay(repo)


@celery.task
def update_popular_repositories(limit: int=1000):
    """Update local data for repositories sorted by most stars & forks."""
    top = Repository.query.order_by(Repository.num_events.desc()).limit(limit)
    update_repositories(top.all())


@celery.task
def update_recently_popular_repositories(prior_days=14, limit: int=500):
    """Update local data for most recently popular repositories."""
    created_at = datetime.date.today() - datetime.timedelta(days=prior_days)
    top = db.session.query(Rating.repo_id) \
        .filter(Rating.created_at >= created_at) \
        .group_by(Rating.repo_id) \
        .order_by(func.COUNT(Rating.repo_id).desc()) \
        .limit(limit)
    repos = Repository.query.filter(Repository.repo_id.in_(top))
    update_repositories(repos.all())


@celery.task
def update_repository(repo, age: int):
    filename = "data/github-api/{}.json.gz".format(repo.repo_id)
    if os.path.exists(filename):
        stats = os.path.getctime(filename)
        if age > time.time() - stats:
            return False
    log.debug("Fetching repository {}".format(repo.name))
    content = get_with_retry(*repo.name.split("/"))
    with gzip.open(filename, "wb") as gz:
        gz.write(content)
    return True


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
        log.info("Sleeping for {}".format(reset))
        time.sleep(reset)
        rv = get_with_retry(user, name)
    return rv
