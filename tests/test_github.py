import json
import random

import responses
import unittest

from growser.tasks import github


class GithubAPITestCase(unittest.TestCase):
    base_url = 'https://api.github.com'

    @responses.activate
    def test_repository(self):
        url = self.base_url + '/repos/twbs/bootstrap'
        body = '{"id": 2126244, "name": "bootstrap", "full_name": "twbs/bootstrap"}'
        responses.add(responses.GET, url, body, content_type='application/json')
        result = json.loads(github.repository('twbs/bootstrap').decode('UTF-8'))
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
        result = json.loads(github.rate_limit().decode("UTF-8"))
        self.assertEqual(responses.calls[0].request.url, url)
        self.assertEqual(result['resources']['core']['remaining'], core)
