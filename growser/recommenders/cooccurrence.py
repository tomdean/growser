import gzip

from numba import njit
import numpy as np
import pandas as pd

from growser.app import log

#: Maximum number of users to include in the user x repo matrix.
MAX_LOGINS = 150000

#: Max number of repositories to include in the user x repo matrix.
MAX_REPOS = 10000


def execute():
    """Use the co-occurrence matrix of a user/item matrix to calculate the
    log-likelihood & jaccard similarity for item/item pairs.

    The Jaccard similarity is just an added bonus that happens to use the same
    parameters as log likelihood."""
    log.info("Loading ratings.csv")
    ratings = pd.read_csv('data/csv/ratings.csv', header=None,
                          names=['login_id', 'repo_id', 'rating', 'date'],
                          usecols=['login_id', 'repo_id'])
    ratings['value'] = 1

    log.info("Filtering users & repositories")
    top_users = ratings.groupby('login_id')['repo_id'].count() \
        .sort_values(ascending=False)
    top_users_filtered = top_users[(top_users >= 10) & (top_users <= 500)]
    top_users_sample = top_users_filtered.sample(MAX_LOGINS)

    top_repos = ratings[ratings['login_id'].isin(top_users_sample.index)] \
        .groupby('repo_id')['login_id'].count() \
        .sort_values(ascending=False)[:MAX_REPOS]

    log.info("Filtering ratings")
    rv = ratings[(ratings['login_id'].isin(top_users_sample.index)) &
                 (ratings['repo_id'].isin(top_repos.index))]

    log.info("Creating user x repo matrix")
    df = rv.pivot(index='repo_id', columns='login_id', values='value').fillna(0)

    del ratings
    del rv

    log.info("Creating co-occurrence matrix (A'A)")
    coo = df.dot(df.T)

    repos = top_repos.index[:20000]
    num_users = df.shape[1]

    def llr(a, b):
        score = log_likelihood(
            coo[a][b],
            coo[b][b],
            coo[a][a],
            num_users - coo[a][a] - coo[b][b]
        )
        return 3, a, b, score

    def jaccard(a, b):
        score = jaccard_score(coo[a][a], coo[b][b], coo[a][b])
        return 2, a, b, score

    log.info("Jaccard similarity")
    _recommendations(repos, coo, jaccard, 'co-occurrence.jaccard')

    log.info("Log-likelihood similarity")
    _recommendations(repos, coo, llr, 'co-occurrence.log-likelihood')


def _recommendations(repos, coo, result, name):
    """
    :param repos: List of repository IDs to generate recommendations for.
    :param coo: Co-occurrence matrix A'A where A is a user x item matrix.
    :param result: Function taking two repository IDs and returns a score.
    :param name: Name of CSV file to save recommendation results to.
    """
    results = []
    for idx, id1 in enumerate(repos):
        scores = coo[id1][coo[id1] >= 5].index.map(lambda id2: result(id1, id2))
        scores = sorted(scores, key=lambda x: x[3], reverse=True)
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
def log_likelihood(k11, k01, k10, k00):
    """Based on the Mahout implementation at http://bit.ly/1NtLSrc.

    :param k11: Number of times the two events occurred together
    :param k01: Number of times the second event occurred w/o the first event
    :param k10: Number of times the first event occurred w/o the second event
    :param k00: Number of times neither event occurred.
    """
    row_entropy = entropy(k11 + k01, k10 + k00)
    col_entropy = entropy(k11 + k10, k01 + k00)
    mat_entropy = entropy(k11, k01, k10, k00)
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
