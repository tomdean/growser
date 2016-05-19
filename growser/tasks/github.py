from datetime import timedelta
import gzip
import json
import time
import os

from celery import group
import requests
from sqlalchemy.sql import and_, exists, func, or_

from growser.app import app, db, celery, log
from growser.models import Repository, RepositoryTask


class ProcessResponse:
    def __init__(self, repo, **kwargs):
        self.repo = repo
        self.data = kwargs


class UpdateRepository(ProcessResponse):
    pass


class IgnoreRepository(ProcessResponse):
    pass


class NotFoundRepository(ProcessResponse):
    pass


@celery.task
def process_repository(repo):
    data = {}
    klass = UpdateRepository

    cached, rv = _update(repository, "repos", repo, 100)
    if rv and b'message' in rv and (b'Not Found' in rv or b'block' in rv):
        klass = NotFoundRepository
    elif cached:
        klass = IgnoreRepository
    else:
        js = json.loads(rv.decode('utf-8'))
        data = {
            'repo_id': int(repo.repo_id),
            'description': js['description'] or '',
            'homepage': js['homepage'] or '',
            'language': js['language'] or '',
            'num_stars': int(js['watchers']),
            'num_forks': int(js['forks']),
            'num_watchers': int(js['subscribers_count'])
        }
        if data['homepage'] != '' and data['homepage'][:4] != 'http':
            data['homepage'] = "http://" + data['homepage']

    return klass(repo, **data)


def update_eligible_repositories_from_github(batch_size: int=100):
    """Find repositories that have not been updated recently."""
    tasks = and_(RepositoryTask.name == 'github.api.repos',
                 RepositoryTask.repo_id == Repository.repo_id,
                 RepositoryTask.created_at >= func.now() - timedelta(days=30))

    query = Repository.query \
        .filter(Repository.status == 1) \
        .filter(Repository.language == '') \
        .filter(or_(Repository.num_events >= 100, Repository.num_stars >= 100)) \
        .filter(~exists().where(tasks)) \
        .order_by(Repository.num_events.desc(), Repository.created_at.desc())

    candidates = query.limit(batch_size).all()
    if not len(candidates):
        log.info('No candidates found')
        return

    log.info("Evaluating %s repositories", len(candidates))

    repos = map(process_repository.s, candidates)
    result = group(repos).apply_async()
    results = result.get(interval=0.25)

    handle_updates([r for r in results if isinstance(r, UpdateRepository)])
    handle_not_found([r for r in results if isinstance(r, NotFoundRepository)])


def handle_updates(results):
    """Update the database with data from the GitHub API."""
    import pandas as pd

    if not len(results):
        return

    updates = [r.data for r in results if r.data]
    log.info("Updating %s repositories", len(updates))
    pd.DataFrame(updates).to_sql('repos_tmp', db.engine, index=False, if_exists='replace')
    db.engine.execute(open("deploy/etl/sql/update_repositories.sql").read())


def handle_not_found(results):
    """Update multiple repositories and set as not found (status = 2)"""
    if not len(results):
        return

    log.info("Setting %s repositories as not found", len(results))
    for result in results:
        db.engine.execute("UPDATE repository SET status=2 WHERE repo_id={}".format(result.repo.repo_id))


def _update(func_api, folder, repo, age):
    path = "data/github-api/{}/{}.json.gz".format(folder, repo.hashid)
    if os.path.exists(path) and age > time.time() - os.path.getctime(path):
        return True, gzip.open(path, 'rb').read()

    log.debug("Fetching {} {}".format(folder, repo.name))
    content = get_with_retry(func_api, repo.name)
    with gzip.open(path, "wb") as gz:
        gz.write(content)

    return False, content


def get_rate_limit():
    """Return the current rate limit from the API."""
    rate = json.loads(rate_limit().decode('UTF-8'))['resources']
    if rate['search']['remaining'] == 0:
        return rate['search']['reset']
    return rate['core']['reset']


def get_with_retry(func, *params) -> str:
    """Wrapper for API requests to GitHub API with rate limit support."""
    rv = func(*params)
    if b'rate limit exceeded' in rv:
        reset = get_rate_limit() - int(time.time())
        log.info("Sleeping for {}".format(reset))
        time.sleep(reset)
        rv = get_with_retry(func, *params)
    return rv


USER_AGENT = "Growser/0.1 (+https://github.com/tomdean/growser)"


def fetch(path, params=None):
    data = requests.get('https://api.github.com/' + '/'.join(path), params,
                        auth=app.config.get('GITHUB_OAUTH'),
                        headers={'User-Agent': USER_AGENT})
    return data.content


def repository(name):
    return fetch(['repos', name])


def releases(name):
    return fetch(['repos', name, 'releases'])


def languages(name):
    return fetch(['repos', name, 'languages'])


def tags(name):
    return fetch(['repos', name, 'tags'])


def rate_limit():
    return fetch(['rate_limit'])
