import datetime

import pandas as pd
from sqlalchemy import func

from growser.models import db, Rating, Repository


def daily_events_by_repo(repo_id):
    date_field = func.DATE(Rating.created_at)
    query = db.session.query(date_field.label("date"),
                             func.COUNT(Rating.rating).label("num_events")) \
        .filter(Rating.repo_id == repo_id) \
        .group_by(date_field) \
        .order_by(date_field)

    df = pd.DataFrame(__to_dict(query.all()))
    if not len(df):
        return []

    df['num_events_m30'] = pd.rolling_mean(df['num_events'], 30).fillna(0)
    df['num_events_avg'] = df['num_events'].mean()
    return df.to_dict("records")


def top_organizations(limit: int):
    rv = db.session.query(
            Repository.organization,
            func.SUM(Repository.num_unique).label("num_unique"),
            func.SUM(Repository.num_events).label("num_events"),
            func.COUNT(Repository.repo_id).label("num_projects")) \
        .group_by(Repository.organization)\
        .order_by(func.SUM(Repository.num_unique).desc()) \
        .limit(limit) \
        .all()
    return [model_to_dict(r) for r in rv]


def model_to_dict(row):
    return dict((col, getattr(row, col))
                for col in row.keys())


def __to_dict(rows):
    rv = []
    for row in rows:
        row = row._asdict()
        for k, v in row.items():
            if type(v) is datetime.date:
                row[k] = v.strftime("%Y-%m-%d")
        rv.append(row)
    return rv
