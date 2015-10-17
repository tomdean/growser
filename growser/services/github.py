from growser.app import app
from requests import get


def fetch(path, params=None):
    data = get('https://api.github.com/' + '/'.join(path),
               auth=app.config.get('GITHUB_OAUTH'), params=params)
    return data.content.decode('utf-8')


def repository(user, name):
    return fetch(['repos', user, name])


def stargazers(user, repo, page=1, per_page=100):
    return fetch(['repos', user, repo, 'stargazers'],
                 {'per_page': per_page, 'page': page})


def search(source, query, sort='stars', order='desc', page=1, per_page=100):
    path = ['search', source]
    params = {'q': query, 'sort': sort, 'order': order,
              'per_page': per_page, 'page': page}
    return fetch(path, params)


def rate_limit():
    return fetch(['rate_limit'])
