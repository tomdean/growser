import pandas as pd

from growser.models import db


BATCH_SIZE = 100000


def initialize_schema():
    db.drop_all()
    db.create_all()


def repositories():
    df1 = pd.read_csv('data/csv/repos.csv')
    df2 = pd.read_csv('data/github/repos-meta.csv').drop('created_at', axis=1)
    fields = [
        'repo_id',
        'name',
        'language',
        'description',
        'num_events',
        'num_unique',
        'created_at'
    ]
    df = pd.merge(df1, df2, left_on="name", right_on="full_name", how="left")
    df = df[fields].copy().drop_duplicates()
    df['language'] = df['language'].fillna('')
    df['description'] = df['description'].fillna('')
    df['created_at'] = pd.to_datetime(df['created_at'], unit='s')
    df.to_csv('data/csv/repositories.csv', index=False)

    session = db.session()
    session.execute("COPY repository FROM '/data/csv/repositories.csv' DELIMITER ',' CSV HEADER")
    session.commit()


def logins():
    session = db.session()
    session.execute("COPY login FROM '/data/csv/logins.csv' DELIMITER ',' CSV HEADER")
    session.commit()


def ratings():
    session = db.session()
    session.execute("COPY rating FROM '/data/csv/rating.csv' DELIMITER ',' CSV HEADER")
    session.commit()

