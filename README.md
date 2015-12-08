# Growser

<img src="static/screenshot.gif" align="right" hspace="10" vspace="6" />

Explore GitHub projects, find recommendations, and visualize trends over time.

## Projects

The following (mostly) open source projects are used to generate recommendations based on user stars and forks.

1. [Apache Mahout 0.9](http://mahout.apache.org/) (Hadoop not required)
1. [Apache Spark 1.5.2](http://spark.apache.org/)
1. [PredictionIO 0.9.5](https://prediction.io/)
1. [GraphLab Create 1.7.1](https://dato.com/products/create/)

GraphLab is the only commercial product in the list, and included

## Requirements

1. Docker >= 1.9
1. Docker Machine => 0.5

## Setup

Please refer to [SETUP.md](SETUP.md) for instructions on installing, configuring, and running the Growser initialization & setup process.

1. Install dependencies
1. Build and start containers
1. Run bootstrap
    1. Download GitHub Archive data from Google BigQuery
    1. Process data into intermediary output (CSV)
    1. Copy data into Postgres
    1. Update project rankings

#### Google Cloud

Google Cloud is required to download data from [GitHub Archive public dataset](https://www.githubarchive.org/).

1. Create [Google Cloud](https://cloud.google.com/) account
1. Enable [Cloud Storage](https://cloud.google.com/storage/docs/signup)
1. Enable [BigQuery](https://cloud.google.com/bigquery/sign-up)
1. Create a [Service Account](https://console.developers.google.com/apis/credentials/serviceaccount/) and export a JSON key

#### GitHub API

A GitHub [personal access token](https://github.com/settings/tokens) is not required for API access, but [limits](https://developer.github.com/v3/rate_limit/) the number of requests to 60/hour. This makes requesting API for 25,000+ repositories untenable.

