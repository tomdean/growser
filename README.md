# Growser
> An engine for the visual discovery of open source projects on Github.com.

<img src="static/header.png" hspace="0" />

## Requirements

1. Python 3.5+
1. Docker
1. 8GB RAM
1. 20GB+ disk space

## Setup

Growser relies on data from [GitHub Archive](https://www.githubarchive.org/),
and requires a Google Cloud account to download data from BigQuery.

1. Create [Google Cloud](https://cloud.google.com/) account
1. Enable [BigQuery](https://cloud.google.com/bigquery/sign-up)
1. Enable [Cloud Storage](https://cloud.google.com/storage/docs/signup) (temporary storage between BigQuery and local)
1. Create a [Service Account](https://console.developers.google.com/apis/credentials/serviceaccount/) and export a JSON key

### GitHub API

A GitHub [personal access token](https://github.com/settings/tokens) is not required for API access, but requests are [limited](https://developer.github.com/v3/rate_limit/) to 60/hour otherwise.

## Experiment

Growser was originally created to experiment with:

1. Docker
1. Machine Learning & Statistics
1. Command Query Responsibility Separation (CQRS)
1. Event Sourcing & Event Stores

### Docker

Growser relies on several open source projects which can be configured using
official Docker containers.

1. [Python / Gunicorn](https://hub.docker.com/_/python/)
1. [Nginx](https://hub.docker.com/_/nginx/)
1. [Postgres 9.4](https://hub.docker.com/_/postgres/)
1. [Redis](https://hub.docker.com/_/redis/)
1. [Celery](https://hub.docker.com/_/celery/)

### Machine Learning

Growser uses collaborative filtering for recommendations and other ML techniques
for identifying trending projects. This includes many 3rd-party libraries:

1. [scikit-learn](http://scikit-learn.org/stable/)
1. [scikit-image](http://scikit-image.org/)
1. [OpenCV](http://opencv.org/)
1. [Apache Mahout](http://mahout.apache.org/)
1. [Apache Spark MLlib](https://spark.apache.org/docs/1.6.1/mllib-guide.html)

### CQRS

Command Query Responsibility Separation is a collection of software "best practices"
loosely masquerading as a design pattern. Like MVC+CRUD, it's a hodgepodge of other
patterns smashed together such that the whole is greater than its parts.

Greg Young first defined the pattern as:

> “... the recognition that there are differing architectural properties when
looking at the paths for reads and writes of a system. CQRS allows the specialization
of the paths to better provide an optimal solution.”

Growser implements an anemic CQRS-inspired framework with goal of demonstrating
how these patterns can be applied over time.

1. **MVC+CRUD** - Single Read + Write Model, Single Database
    1. Flask VC, SQLAlchemy R+W models
1. **CQS** - Separate Read & Write Models, Single Database
    1. Flask VC, DTO read model from SQLALchemy write model
1. **CQRS** - Separate Read & Write Models, Separate Read & Write Databases
    1. Redis as read-only data storage
1. **CQRS+DDD** - Introduce DDD-style aggregate roots (AR)
    1. Repositories + AR
    1. AR replace command handlers
    1. AR also subscribe to domain events
1. **CQRS+DDD+ES** - Introduce event sourcing

### Event Sourcing & Event Stores

One of the problems of using a relational database as your primary data store is that
any changes to state (UPDATE, DELETE) will result in losing data. This isn't a
problem until you build a data warehouse and realize you have crazy inconsistent
values in your orders table that just should't be possible. But there they are,
the impossible repeating thousands, maybe millions of times.

A BI engineer might consider implementing [type 6 slowly-changing dimensions](http://www.kimballgroup.com/2013/02/design-tip-152-slowly-changing-dimension-types-0-4-5-6-7/)
to tackle this problem, but you still risk losing any data in the window between
snapshots. We've also introduced 4 new objects per table (created/updated column+index).
Not to mention the politicking required to convince a DBA to allow you to make
these changes to production databases in the first place.

Enter event stores and event sourcing.

Instead of doing something like adding triggers to every table, we do something
equally crazy and update our entire application to write to a custom log before
propagating that event to the rest of our system - such as the component
that actually updates the database.

It's a bit similar to what relational databases call transaction logs -
a sequence of events that have been persisted to disk before being
applied to the running state of the system. In Postgres it's implemented as a
[write-ahead log](https://www.postgresql.org/docs/9.5/static/wal-intro.html).
Same for [SQL Server](https://technet.microsoft.com/en-us/library/jj835093(v=sql.110).aspx#WAL)
and [SQLite](https://www.sqlite.org/wal.html).

If the database crashes before transient data can be saved to disk, no sweat.
It was committed to the WAL before being applied to the in-memory running state,
so restoring the last physical copy of the data and applying the WAL to it is
all we need to do.

An event store is essentially a **write-ahead archive** instead of a log.

### Domain-Drive Design

Now that we have an immutable stream of events that represent every change to our
system, we can apply Domain-Drive Design (DDD) to glorious achievement by
introducing aggregate roots, bounded contexts, and domain models.

When combined, ES+DDD becomes the **write** model within CQRS. The event store
provides a sequence of events that can be retrieved by aggregate root ID, and
the aggregate roots contain all of the business logic needed that when given a
stream of events it can restore itself to the current state.
