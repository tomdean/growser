import json
import random
import re

import responses
import unittest

from growser.services import github


class GithubAPITestCase(unittest.TestCase):
    base_url = 'https://api.github.com'

    @responses.activate
    def test_repository(self):
        url = self.base_url + '/repos/twbs/bootstrap'
        body = '{"id": 2126244, "name": "bootstrap", "full_name": "twbs/bootstrap"}'
        responses.add(responses.GET, url, body, content_type='application/json')
        result = json.loads(github.repository('twbs', 'bootstrap'))
        body_js = json.loads(body)
        self.assertEqual(responses.calls[0].request.url, url)
        self.assertEqual(body_js['id'], result['id'])

    @responses.activate
    def test_rate_limit(self):
        url = self.base_url + '/rate_limit'
        core = random.randint(40, 60)
        body = json.dumps({
            "resources": {
                "core": {"limit": 60, "remaining": core, "reset": 1445038877},
                "search": {"limit": 10, "remaining": 10, "reset": 1445036211}
            },
            "rate": {"limit": 60, "remaining": 60, "reset": 1445038877}
        })
        responses.add(responses.GET, url, body, content_type='application/json')
        result = json.loads(github.rate_limit())
        self.assertEqual(responses.calls[0].request.url, url)
        self.assertEqual(result['resources']['core']['remaining'], core)

    @responses.activate
    def test_stargazers(self):
        url = re.compile(self.base_url + "/repos/twbs/bootstrap/stargazers")
        body = '[{"login": "twbs", "id": 77083}]'
        responses.add(responses.GET, url, body, content_type='application/json')
        response = github.stargazers("twbs", "bootstrap")
        self.assertEqual(response, body)

    @responses.activate
    def test_search(self):
        url = re.compile(self.base_url + "/search/repositories")
        body = '{"total_count":19225,"incomplete_results":false,"items":[{"id":843222,"name":"scikit-learn","full_name":"scikit-learn/scikit-learn"}]}'

        responses.add(responses.GET, url, body, content_type='application/json')

        page = random.randint(1, 5)
        per_page = random.randint(50, 100)
        github.search('repositories', 'machine learning',
                      page=page, per_page=per_page)

        assert "per_page={}".format(per_page) in responses.calls[0].request.url
        assert "page={}".format(page) in responses.calls[0].request.url
