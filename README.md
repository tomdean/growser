# Growser

<img src="static/screenshot.gif" align="right" hspace="10" vspace="6" />

Explore GitHub projects, find recommendations, and visualize popularity over time.

## Projects

The following (mostly) open source projects are used to generate recommendations based on user stars and forks.

1. [Apache Mahout 0.9](http://mahout.apache.org/) (pre-Spark)
1. [Apache Spark 1.5.1](http://spark.apache.org/)
1. [PredictionIO 0.9.5](https://prediction.io/)
1. [GraphLab Create 1.6.1](https://dato.com/products/create/)

## Requirements

1. Python >= 3.4
1. Docker >= 1.9
1. Docker Machine => 0.5
1. Google Cloud

## Setup

#### Google Cloud

Google Cloud is required to download data from [GitHub Archive public dataset](https://www.githubarchive.org/).

1. Create [Google Cloud](https://cloud.google.com/) account
1. Enable [Cloud Storage](https://cloud.google.com/storage/docs/signup)
1. Enable [BigQuery](https://cloud.google.com/bigquery/sign-up)
1. Create a [Service Account](https://console.developers.google.com/apis/credentials/serviceaccount/) and export a JSON key

#### GitHub API

A GitHub [personal access token](https://github.com/settings/tokens) is not required for API access, but [limits](https://developer.github.com/v3/rate_limit/) the number of requests to 60/hour.

