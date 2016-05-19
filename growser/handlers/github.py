
import json
import time


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