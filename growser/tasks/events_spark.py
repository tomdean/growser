from pyspark import SparkContext, SparkConf
from pyspark.serializers import MarshalSerializer
from pyspark.sql import HiveContext
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

conf = SparkConf() \
    .setAppName("Process GitHub Events") \
    .setMaster("local[8]")
sc = SparkContext(conf=conf, serializer=MarshalSerializer())
sql_ctx = HiveContext(sc)


def convert_row(row):
    """Return (k,v) tuple: (login, repo) -> (rating, date, count)"""
    if len(row) == 7:
        return (row[4], row[3]), (int(row[0]), int(int(row[6])/1000000), 1)
    return (row[3], row[2]), (int(row[0]), int(int(row[4])/1000000), 1)


def save(df, path: str):
    df.write.format('com.databricks.spark.csv') \
        .options(header="true").save(path)


def process_events_dataframes(path: str):
    """Process the Github Archive logs using Apache Spark & DataFrames."""
    rdd = sc.textFile(path) \
        .map(lambda x: x.split(",")) \
        .filter(lambda x: x[0] != "type") \
        .map(lambda x: convert_row(x))

    # De-deduplicate by (login, repo) -> (rating, min(date), count)
    reduced = rdd.reduceByKey(
        lambda x, y: (x[0] | y[0], min(x[1], y[1]), x[2] + y[2]))
    ratings = reduced.map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1]))

    schema = StructType([
        StructField("login", StringType(), False),
        StructField("repo", StringType(), False),
        StructField("rating", IntegerType(), False),
        StructField("timestamp", IntegerType(), False)
    ])
    df = sql_ctx.createDataFrame(ratings, schema)
    df.registerTempTable("ratings")

    # Order by timestamp/login so that we can generate predictable login IDs
    logins = sql_ctx.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY timestamp ASC, login ASC) AS login_id,
            login
        FROM (
            SELECT login, MIN(timestamp) AS timestamp
            FROM ratings
            GROUP BY login
        ) AS tmp
    """)
    logins.registerTempTable("logins")

    repos = sql_ctx.sql("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY timestamp ASC, repo ASC) AS repo_id,
            repo
        FROM (
            SELECT repo, MIN(timestamp) AS timestamp
            FROM ratings
            GROUP BY repo
        ) AS tmp
    """)
    repos.registerTempTable("repos")

    final = sql_ctx.sql("""
        SELECT
            l.login_id,
            rr.repo_id,
            r.rating,
            r.timestamp
        FROM ratings AS r
        JOIN logins AS l ON l.login = r.login
        JOIN repos AS rr ON rr.repo = r.repo
    """)

    save(final, 'data/csv/ratings/')
    save(logins, 'data/csv/logins/')
    save(repos, 'data/csv/repos/')


def process_events(path):
    """Process using standard RDD operations."""
    rdd = sc.textFile(path) \
        .map(lambda x: x.split(",")) \
        .filter(lambda x: x[0] != "type") \
        .map(lambda x: convert_row(x))

    # Logins can star/fork a repository multiple times - reduce until we have a
    # single row: (login, repo) -> (rating, min(date), count)
    reduced = rdd.reduceByKey(
        lambda x, y: (x[0] | y[0], min(x[1], y[1]), x[2] + y[2]))

    logins = reduced.map(lambda a: (a[0][0], a[1][1])) \
        .reduceByKey(lambda a, b: min(a, b)) \
        .sortBy(lambda x: (x[1], x[0])) \
        .map(lambda x: x[0]).zipWithIndex() \
        .toDF(["login", "login_id"])

    # Do the same for repositories
    repos = reduced.map(lambda a: (a[0][1], a[1][1])) \
        .reduceByKey(lambda a, b: min(a, b)) \
        .sortBy(lambda x: (x[1], x[0])) \
        .map(lambda x: x[0]).zipWithIndex() \
        .toDF(["repo", "repo_id"])

    # (login, repo, rating, timestamp)
    ratings = reduced.map(lambda a: (a[0][0], a[0][1], a[1][0], a[1][1])) \
        .toDF(["login", "repo", "rating", "timestamp"])

    # Easier to join using SQL
    ratings.registerTempTable("ratings")
    logins.registerTempTable("logins")
    repos.registerTempTable("repos")

    final = sql_ctx.sql("""
        SELECT
            l.login_id,
            rr.repo_id,
            r.rating,
            r.timestamp
        FROM ratings AS r
        JOIN logins AS l ON l.login = r.login
        JOIN repos AS rr ON rr.repo = r.repo
    """)

    save(final, 'data/csv/ratings/')
    save(logins, 'data/csv/logins/')
    save(repos, 'data/csv/repos/')
