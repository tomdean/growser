from datetime import datetime
import gzip

from numba import njit
import numpy as np
import pandas as pd

from growser.app import log

#: Maximum number of users to include in the user/repo matrix.
MAX_LOGINS = 100000


def run_recommendations(ratings: str, output: str, num_repos: int):
    ratings = fetch_ratings(ratings, None, num_repos)

    log.info("Creating co-occurrence matrix (A'A)")
    coo = ratings.dot(ratings.T)

    log.info("Log-likelihood similarity")
    _recommendations(4, ratings.shape[1], ratings.index, coo, score_llr,
                     'co-occurrence.log-likelihood')

    log.info("Jaccard similarity")
    _recommendations(6, ratings.shape[1], ratings.index, coo, score_jaccard,
                     'co-occurrence.jaccard')


def fetch_ratings(filename: str, num_repos: int):
    log.info("Loading %s", filename)
    ratings = pd.read_csv(filename, header=None,
                          names=['login_id', 'repo_id', 'rating', 'date'])
    ratings['value'] = 1

    log.info("Filtering ratings")
    top_users = ratings.groupby('login_id')['repo_id'].count() \
        .sort_values(ascending=False) \
        .sample(MAX_LOGINS)

    top_repos = ratings[ratings['login_id'].isin(top_users.index)] \
        .groupby('repo_id')['login_id'].count() \
        .sort_values(ascending=False)[:num_repos]

    rv = ratings[(ratings['login_id'].isin(top_users.index)) &
                 (ratings['repo_id'].isin(top_repos.index))]

    log.info("Creating user/repo matrix")
    df = rv.pivot(index='repo_id', columns='login_id', values='value').fillna(0)

    return df


def score_llr(coo, num_interactions, a, b):
    score = log_likelihood(
        coo[a][b],
        coo[a][a] - coo[a][b],
        coo[b][b] - coo[a][b],
        num_interactions - coo[a][a] - coo[b][b] + coo[a][b]
    )
    return [a, b, score]


def score_jaccard(coo, num_interactions, a, b):
    score = jaccard_score(coo[a][a], coo[b][b], coo[a][b])
    return [a, b, score]


def _recommendations(model_id, num_interactions, repos, coo, result, name):
    """
    :param repos: List of repository IDs to generate recommendations for.
    :param coo: Co-occurrence matrix A'A where A is a user x item matrix.
    :param result: Function taking two repository IDs and returns a score.
    :param name: Name of CSV file to save recommendation results to.
    """
    results = []
    for idx, id1 in enumerate(repos):
        scores = coo[id1][coo[id1] >= 5].index.map(
            lambda id2: [model_id] + result(coo, num_interactions, id1, id2))
        scores = sorted(scores, key=lambda x: x[3], reverse=True)
        # Exclude first result (since it will be id1)
        results += scores[1:101]
        if idx > 0 and idx % 100 == 0:
            log.debug("Finished {}".format(idx))
    save_csv(name, results)


def save_csv(filename, results):
    filename = 'data/recommendations/python/{}.csv.gz'.format(filename)
    with gzip.open(filename, 'wb') as csv:
        for row in results:
            csv.write((",".join(map(str, row)) + "\n").encode('utf-8'))


@njit(nogil=True)
def xlogx(x):
    return 0 if x == 0 else x * np.log(x)


@njit(nogil=True)
def entropy(*args):
    """Calculates the un-normalized shannon entropy based on:

        :math:`-sum x_i log x_i / N = -N sum x_i/N log x_i/N`

    Where :math:`N = sum x_i`
    """
    total = 0
    total_xlogx = 0
    for x in args:
        total += x
        total_xlogx += xlogx(x)
    return xlogx(total) - total_xlogx


@njit(nogil=True)
def log_likelihood(k11, k12, k21, k22):
    """Based on the Mahout implementation at http://bit.ly/1NtLSrc.

    :param k11: Number of times the two events occurred together
    :param k12: Number of times the first event occurred w/o the second event
    :param k21: Number of times the second event occurred w/o the first event
    :param k22: Number of times neither event occurred.
    """
    row_entropy = entropy(k11 + k12, k21 + k22)
    col_entropy = entropy(k11 + k21, k12 + k22)
    mat_entropy = entropy(k11, k12, k21, k22)
    if mat_entropy > row_entropy + col_entropy:
        return 0.0
    llr = 2.0 * (row_entropy + col_entropy - mat_entropy)
    return 1.0 - 1.0 / (1.0 + llr)


@njit(nogil=True)
def jaccard_score(a, b, ab):
    """Calculates the Jaccard similarity coefficient between two repositories.

    :param a: Number of times the second event occurred w/o the first event
    :param b: umber of times the first event occurred w/o the second event
    :param ab: Number of times the two events occurred together
    """
    return ab / (a + b - ab)
