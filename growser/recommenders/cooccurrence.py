import gzip

from numba import njit
import numpy as np
import pandas as pd

from growser.app import app


def setup_recommendations():
    app.logger.info("Loading ratings.csv")
    ratings = pd.read_csv('data/csv/ratings.csv', header=None,
                          names=['login_id', 'repo_id', 'rating', 'date'],
                          usecols=['login_id', 'repo_id'])
    ratings['rating'] = 1

    app.logger.info("Filtering users & repositories")
    top_users = ratings.groupby('login_id')['repo_id'].count() \
        .sort_values(ascending=False)

    top_users_filtered = top_users[(top_users >= 10) & (top_users <= 250)]
    top_users_sample = top_users_filtered.sample(150000)

    top_repos = ratings[ratings['login_id'].isin(top_users_sample.index)] \
        .groupby('repo_id')['login_id'].count() \
        .sort_values(ascending=False)[:10000]

    app.logger.info("Filtering ratings")
    rv = ratings[(ratings['login_id'].isin(top_users_sample.index)) &
                 (ratings['repo_id'].isin(top_repos.index))]

    app.logger.info("Creating user x repo matrix")
    df = rv.pivot(index='repo_id', columns='login_id', values='rating').fillna(0)

    del ratings
    del rv

    app.logger.info("Creating co-occurrence matrix (A'A)")
    coo = df.dot(df.T)
    recommendations_for = top_repos.index[:10000]

    app.logger.info("Jaccard similarity")
    jaccard_similarity(coo, recommendations_for)

    app.logger.info("Log-likelihood similarity")
    llr_similarity(coo, df.shape[1], recommendations_for)

    app.logger.info("Done")


def llr_similarity(coo: pd.DataFrame, num_samples: int, repos: list):
    model_id = 3

    def score(a, b):
        llr = log_likelihood(
            coo[a][b],
            coo[b][b],
            coo[a][a],
            num_samples - coo[a][a] - coo[b][b]
        )
        return model_id, a, b, llr

    generate_recommendations(coo, repos, score, 'cooccurrence.loglikelihood')


def jaccard_similarity(coo: pd.DataFrame, repos: list):
    model_id = 2

    def score(a, b):
        return model_id, a, b, jaccard_score(coo[a][a], coo[b][b], coo[a][b])

    generate_recommendations(coo, repos, score, 'jaccard')


def generate_recommendations(coo, repos, score_func, name):
    results = []
    for idx, id1 in enumerate(repos):
        recommend = coo[id1][coo[id1] >= 5].index
        scores = sorted(recommend.map(lambda id2: score_func(id1, id2)),
                        key=lambda x: x[3], reverse=True)
        results += scores[1:101]
        if idx > 0 and idx % 100 == 0:
            app.logger.debug("Finished {}".format(idx))
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
    total = 0
    total_log = 0
    for x in args:
        total += x
        total_log += xlogx(x)
    return xlogx(total) - total_log


@njit(nogil=True)
def log_likelihood(k11, k12, k21, k22):
    """
    @param k11: Number of times the two events occurred together
    @param k12: Number of times the second event occurred w/o the first event
    @param k21: Number of times the first event occurred w/o the second event
    @param k22: Number of times something else occurred
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
    return ab / (a + b - ab)


if __name__ == '__main__':
    setup_recommendations()
