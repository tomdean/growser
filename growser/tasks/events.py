import glob
import numpy as np
import pandas as pd

from growser.app import log

#: Minimum number of stars & forks a repository needs to be included
MIN_REPO_EVENTS = 25

#: Minimum number of events a user must have before including them
MIN_LOGIN_EVENTS = 10

#: Some users don't have refined taste. Also bots.
MAX_LOGIN_EVENTS = 1000


def process_events(path: str="data/events/events_*.gz"):
    """Process the entirety of the Github-Archive files.

    :param path: Location of the gzip archives
    """
    log.info("Processing GitHub Archive files")
    events = _get_events_dataframe(path)

    # Find all logins & repos and assign them unique IDs
    log.info("Finding unique repositories & logins")
    repositories = _unique_values(events, 'repo', 'login')
    logins = _unique_values(events, 'login', 'repo')

    # Filter logins & repos to prune outliers and items with too little activity
    log.info("Filter events to repos & logins with min # of events")
    min_repo = repositories[repositories['num_unique'] >= MIN_REPO_EVENTS]
    min_login = logins[(logins['num_unique'] >= MIN_LOGIN_EVENTS) &
                       (logins['num_unique'] <= MAX_LOGIN_EVENTS)]

    # Update events to use numeric repo/login IDs
    log.info("Joining events to repos & logins")
    fields = ['type', 'repo_id', 'login_id', 'created_at_x']
    events = pd.merge(events, min_repo, on='repo')
    events = pd.merge(events, min_login, on='login')[fields] \
        .rename(columns={'created_at_x': 'created_at'})

    # Eliminate dupes by grouping by type
    log.info("Group per user/repo/type")
    per_type = events.groupby(['login_id', 'repo_id', 'type']) \
        .agg({'created_at': 'min'}) \
        .reset_index()

    # If a user has starred and forked a repo, rating will be 3
    log.info("Group per user/repo")
    final = per_type.groupby(['login_id', 'repo_id']) \
        .agg({'type': 'sum', 'created_at': 'min'}) \
        .reset_index() \
        .rename(columns={'type': 'rating'})

    log.info("Writing results to CSV")
    repositories = repositories.rename(columns={'repo': 'name'})
    ratings = final[['login_id', 'repo_id', 'rating', 'created_at']].copy(False)

    def to_csv(df: pd.DataFrame, name: str):
        df.to_csv("data/csv/{}.csv".format(name), header=True, index=False)

    to_csv(repositories, 'repos')
    to_csv(logins, 'logins')
    to_csv(ratings, 'ratings')

    # Make a copy to import into SQL
    ratings['created_at'] = pd.to_datetime(final['created_at'], unit='s')
    to_csv(ratings, 'ratings.datetime')


def _unique_values(df: pd.DataFrame, column: str, unique: str) -> pd.DataFrame:
    # Extract unique values from a `pandas.Series` sorted by earliest timestamp
    rv = df.groupby(column) \
        .agg({'created_at': [np.amin, np.count_nonzero],
              unique: lambda x: len(x.unique())}) \
        .reset_index()
    rv.columns = rv.columns.droplevel(0)
    rv = rv.rename(columns={
        '': column,
        '<lambda>': 'num_unique',
        'amin': 'created_at',
        'count_nonzero': 'num_events'
    })
    columns = [column + '_id', column, 'num_events', 'num_unique', 'created_at']
    return rv.sort_values(['created_at', column]) \
        .reset_index(drop=True) \
        .reset_index() \
        .rename(columns={'index': column + '_id'})[columns]


def _get_renamed_repositories() -> pd.DataFrame:
    """Returns a mapping of repositories that have been renamed"""
    rv = open("data/github/redirects.csv").read().split("\n")
    rv = [r for r in map(lambda x: x.split(","), rv) if len(r) == 2]
    return pd.DataFrame(rv, columns=["repo", "to"])


def _get_events_dataframe(path: str) -> pd.DataFrame:
    files = glob.glob(path)
    fields = ['type', 'repo', 'login', 'created_at']
    dtypes = {'type': np.int, 'repo': np.object,
              'login': np.object, 'created_at': np.long}
    rv = []
    renamed = _get_renamed_repositories()
    for file in files:
        log.debug("Processing " + file)
        df = pd.read_csv(file, engine='c', usecols=fields, dtype=dtypes)
        df = pd.merge(df, renamed, how="left", on="repo")
        df['created_at'] = (df['created_at'] / 1000000).astype(np.int)
        df['repo'] = df['to'].fillna(df['repo'])
        df.drop(['to'], axis=1, inplace=True)
        rv.append(df)
    log.info("Concatenating DataFrames")
    return pd.concat(rv)
