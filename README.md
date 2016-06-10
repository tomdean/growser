# Growser
> An engine for the visual discovery of open source projects on Github.com.

<img src="static/header.png" hspace="0" />

## Requirements

1. Python 3.5+
1. Docker
1. 8GB RAM
1. 20GB+ disk (CSV + Postgres)

## Setup

Growser uses data from [GitHub Archive](https://www.githubarchive.org/) and
requires a Google Cloud account.

1. Create [Google Cloud](https://cloud.google.com/) account
1. Enable [BigQuery](https://cloud.google.com/bigquery/sign-up)
1. Enable [Cloud Storage](https://cloud.google.com/storage/docs/signup)
1. Create a [Service Account](https://console.developers.google.com/apis/credentials/serviceaccount/) and export a JSON key

### GitHub API

GitHub API requests are [throttled](https://developer.github.com/v3/rate_limit/)
to 60/hour without a [personal access token](https://github.com/settings/tokens).

### Docker

The following official images are used for development.

1. [Python / Gunicorn](https://hub.docker.com/_/python/)
1. [Nginx](https://hub.docker.com/_/nginx/)
1. [Postgres 9.4](https://hub.docker.com/_/postgres/)
1. [Redis](https://hub.docker.com/_/redis/)
1. [Celery](https://hub.docker.com/_/celery/)

