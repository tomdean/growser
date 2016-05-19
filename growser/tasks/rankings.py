import calendar
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import func

from growser.app import celery, db, log
from growser.db import BulkInsertList
from growser.models import Rating, Ranking, RankingPeriod, Repository

#: Number of repositories to include in the rankings
RANK_TOP_N = 1000

ALL_LANGUAGES = "All"


@celery.task
def update_rankings(language: str, period: str, start_date: date,
                    end_date: date, limit: int = RANK_TOP_N):
    """Update rankings for an arbitrary"""
    log.info("Rankings for {}, {}, {}".format(period, language, start_date))

    count = func.COUNT(Rating.repo_id)
    query = db.session.query(Rating.repo_id, count.label("num_events")) \
        .join(Repository, Repository.repo_id == Rating.repo_id) \
        .group_by(Rating.repo_id) \
        .order_by(count.desc())

    if start_date:
        query = query.filter(Rating.created_at >= start_date)
    if end_date:
        query = query.filter(Rating.created_at < end_date)
    if language != ALL_LANGUAGES:
        query = query.filter(Repository.language == language)

    rankings = query.limit(limit).all()

    df = pd.DataFrame(rankings)
    df['language'] = language
    df['period'] = period
    df['date'] = start_date
    df['rank'] = df[df.columns[1]].rank(0, 'min', ascending=False).astype(np.int)

    cleanup = Ranking.query.filter(Ranking.language == language) \
        .filter(Ranking.period == period)

    if period not in (RankingPeriod.AllTime, RankingPeriod.Recent):
        cleanup = cleanup.filter(Ranking.date == start_date)

    cleanup.delete()

    ranked = df.to_records(False)
    batch = BulkInsertList(Ranking.__table__, ranked, list(df.columns))
    batch.execute(db.engine)


def get_top_languages():
    """Top 25 languages by total # of stars & forks."""
    query = db.session.query(Repository.language) \
        .join(Rating, Rating.repo_id == Repository.repo_id) \
        .filter(Repository.language != '') \
        .filter(Rating.created_at >= '2015-10-01') \
        .group_by(Repository.language) \
        .order_by(func.COUNT(Rating.repo_id).desc()) \
        .limit(25)
    return [lang[0] for lang in query.all()]


def to_first_day_of_week(for_date):
    """Return the first day of the week (Sunday) relative to `for_date`."""
    days = for_date.isoweekday()
    if days != 7:
        return for_date - timedelta(days=days)
    return for_date


def weekly_interval(start_date: date, end_date: date):
    start_date = to_first_day_of_week(start_date)
    while end_date > start_date:
        yield start_date
        start_date += timedelta(days=7)


def monthly_interval(start_date: date, end_date: date):
    start_date = start_date.replace(day=1)
    while end_date > start_date:
        yield start_date
        days = calendar.monthrange(start_date.year, start_date.month)[1]
        start_date += timedelta(days=days)
