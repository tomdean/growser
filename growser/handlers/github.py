from datetime import date, timedelta
from functools import partial
import ujson as json
from typing import List, Union

from celery import group
import pandas as pd
from sqlalchemy import and_, exists, func, text

from growser import httpcache
from growser.app import app, db, log
from growser.cmdr import DomainEvent, Handles, handles
from growser.commands.github import (
    BatchUpdateFromGitHubAPI,
    UpdateFromGitHubAPI
)
from growser.models import Repository, RepositoryTask, Rating
from growser.tasks import run_command


class RepositoryUpdated(DomainEvent):
    def __init__(self, name: str, description: str, homepage: str,
                 language: str, num_stars: int, num_forks: int,
                 num_watchers: int):
        self.name = name
        self.description = description
        self.homepage = homepage
        self.language = language
        self.num_stars = num_stars
        self.num_forks = num_forks
        self.num_watchers = num_watchers


class RepositoryNotFound(DomainEvent):
    def __init__(self, name):
        self.name = name


@handles(UpdateFromGitHubAPI)
def update_repository(cmd: UpdateFromGitHubAPI) \
        -> Union[RepositoryNotFound, RepositoryUpdated]:
    """Update local repository data from GitHub API."""
    api = GitHubAPIWrapper(app.config.get('GITHUB_OAUTH'), 86400*14)
    rsp = api.repository(cmd.name)

    # Not found or repository blocked/disabled
    if 'message' in rsp and ('Not Found' in rsp['message'] or
                             'block' in rsp['message']):
        return RepositoryNotFound(cmd.name)

    rv = RepositoryUpdated(
        name=cmd.name,
        description=rsp['description'] or '',
        homepage=rsp['homepage'] or '',
        language=rsp['language'] or '',
        num_stars=int(rsp['watchers']),
        num_forks=int(rsp['forks']),
        num_watchers=int(rsp['subscribers_count'])
    )

    if '.' in rv.homepage and ' ' not in rv.homepage and rv.homepage[:4] != 'http':
        rv.homepage = 'http://' + rv.homepage

    if len(rv.homepage) >= 250:
        rv.homepage = ''

    return rv


class BatchUpdateGitHubAPIHandler(Handles[BatchUpdateFromGitHubAPI]):
    def handle(self, cmd: BatchUpdateFromGitHubAPI):
        """Use Celery to update multiple repositories."""
        api = GitHubAPIWrapper(app.config.get('GITHUB_OAUTH'), 86400*14)
        limit = api.rate_limit()

        if cmd.limit > limit['rate']['remaining']:
            raise ValueError('Exceeds current API limit: {}'.format(limit))

        repos = get_repositories(cmd.limit, cmd.task_window,
                                 cmd.rating_window, cmd.min_events)
        tasks = [UpdateFromGitHubAPI(repo.name) for repo in repos]

        def batched(l, n):
            for i in range(0, len(l), n):
                yield l[i:i+n]

        for batch in batched(tasks, cmd.batch_size):
            self._execute_batch(batch)

    def _execute_batch(self, batch: List[UpdateFromGitHubAPI]):
        """Process a single batch of updates."""
        tasks = map(run_command.s, batch)
        results = group(tasks).apply_async().get(interval=1)

        updated = [r for r in results if isinstance(r, RepositoryUpdated)]
        missing = [r for r in results if isinstance(r, RepositoryNotFound)]

        # This should eventually be moved to an event listener
        log.info('Updating: {}, Missing: {}'.format(len(updated), len(missing)))
        self._handle_updates(updated)
        self._handle_missing(missing)

    def _handle_updates(self, updates: List[RepositoryUpdated]):
        if not len(updates):
            return

        # Use pandas to insert data into a temporary table
        pd.DataFrame([r.__dict__ for r in updates]).to_sql(
            name='repos_tmp', con=db.engine, index=False, if_exists='replace')

        # But merge data manually
        query = open("deploy/etl/sql/update_repositories.sql").read()
        db.engine.execute(text(query))

    def _handle_missing(self, missing: List[RepositoryNotFound]):
        if not len(missing):
            return
        query = "UPDATE repository SET status=2 WHERE name = :name"
        for result in missing:
            db.engine.execute(text(query), name=result.name)

#: Temporary user agent
USER_AGENT = "Growser/0.1 (+https://github.com/tomdean/growser)"


class GitHubAPIWrapper:
    """Wrap requests to the GitHub API through :mod:`.httpcache`."""
    def __init__(self, credentials: tuple, expires: int):
        self._request = partial(httpcache.get,
                                expires=expires, auth=credentials,
                                headers={'User-Agent': USER_AGENT})

    def request(self, path: list, params: dict = None):
        url = 'https://api.github.com/' + '/'.join(path)
        return json.loads(self._request(url=url, params=params))

    def repository(self, name):
        return self.request(['repos', name])

    def releases(self, name):
        return self.request(['repos', name, 'releases'])

    def languages(self, name):
        return self.request(['repos', name, 'languages'])

    def rate_limit(self):
        return self.request(['rate_limit'])


def get_repositories(limit: int, task_days: int, rating_days: int, min_events: int):
    # Exclude recently updated repositories @todo: Move to query
    tasks = and_(
        RepositoryTask.name == 'github.api.repos',
        RepositoryTask.repo_id == Rating.repo_id,
        RepositoryTask.created_at >= func.now() - timedelta(days=task_days)
    )

    # Only update repositories that have recent activity
    query = db.session.query(Rating.repo_id, func.count(1).label('num_events')) \
        .filter(~exists().where(tasks)) \
        .filter(Rating.created_at >= date.today() - timedelta(days=rating_days)) \
        .group_by(Rating.repo_id) \
        .having(func.count(1) >= min_events) \
        .order_by(func.count(1).desc())

    # Return (name, num_events)
    popular = query.subquery()
    candidates = db.session.query(Repository.name, popular.columns.num_events) \
        .filter(Repository.status == 1) \
        .join(popular, popular.columns.repo_id == Repository.repo_id)

    return candidates.limit(limit).all()
