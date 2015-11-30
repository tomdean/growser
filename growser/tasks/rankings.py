import calendar
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import func
from sqlalchemy.sql import delete

from growser.app import app, db
from growser.models import MonthlyRanking, MonthlyRankingByLanguage, Rating, \
    Repository, WeeklyRanking, WeeklyRankingByLanguage
from growser.tasks.database import BulkInsertList

FIRST_START_DATE = date(2012, 4, 1)
RANK_TOP_N = 1000


def to_first_day_of_week(for_date):
    """Return the first day of the week (Sunday) relative to `for_date`."""
    days = for_date.isoweekday()
    if days != 7:
        return for_date - timedelta(days=days)
    return for_date


def update_weekly_rankings(start_date: date):
    """Update rankings for the week beginning on `start_date`."""
    start_date = to_first_day_of_week(start_date)
    end_date = start_date + timedelta(days=7)
    update_rankings(WeeklyRanking.__table__, start_date, end_date)


def update_monthly_rankings(start_date: date):
    """Update rankings for the month of `start_date`."""
    start_date = start_date.replace(day=1)
    days = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = start_date + timedelta(days=days)
    update_rankings(MonthlyRanking.__table__, start_date, end_date)


def update_rankings(table, start_date: date, end_date: date):
    app.logger.info("Updating %s for %s", table, start_date)
    query = get_top_repositories(start_date, end_date).limit(RANK_TOP_N)

    for_date = start_date.strftime("%Y-%m-%d")
    repos = map(lambda x: (for_date,) + x, query.all())
    ranked = rank_repositories(list(repos), 2)

    db.engine.execute(delete(table).where(table.c.date == start_date))

    columns = ["date", "repo_id", "num_events", "rank"]
    batch = BulkInsertList(table, ranked, columns)
    batch.execute(db.engine)


def update_weekly_language_rankings(language: str, start_date: date):
    """Update language rankings for the week beginning on `start_date`."""
    for_date = to_first_day_of_week(start_date)
    update_language_rankings(WeeklyRankingByLanguage.__table__, language,
                             start_date, for_date + timedelta(days=7))


def update_monthly_language_rankings(language: str, start_date: date):
    """Update language rankings for the month of `start_date`."""
    start_date = start_date.replace(day=1)
    days = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = start_date + timedelta(days=days)
    update_language_rankings(MonthlyRankingByLanguage.__table__, language,
                             start_date, end_date)


def update_language_rankings(table, language, start_date: date, end_date: date):
    app.logger.info("Updating %s for %s (%s)", table, start_date, language)
    query = get_top_repositories(start_date, end_date) \
        .join(Repository, Repository.repo_id == Rating.repo_id) \
        .filter(Repository.language == language) \
        .limit(RANK_TOP_N)

    for_date = start_date.strftime("%Y-%m-%d")
    repos = map(lambda x: (for_date, language) + x, query.all())
    ranked = rank_repositories(list(repos), 3)

    db.engine.execute(delete(table)
                      .where(table.c.date == start_date)
                      .where(table.c.language == language))
    columns = ["date", "language", "repo_id", "num_events", "rank"]
    batch = BulkInsertList(table, ranked, columns)
    batch.execute(db.engine)


def get_top_languages():
    """Top 25 languages by total # of stars & forks."""
    query = db.session.query(Repository.language) \
        .join(Rating, Rating.repo_id == Repository.repo_id) \
        .filter(Repository.language != '') \
        .group_by(Repository.language) \
        .order_by(func.COUNT(Rating.repo_id).desc()) \
        .limit(25)
    return [lang[0] for lang in query.all()]


def get_top_repositories(start_date, end_date):
    """Most popular repositories by number of stars + forks.

    :param start_date: Include ratings since this date.
    :param end_date: Include ratings until this date."""
    count = func.COUNT(Rating.repo_id)
    return db.session.query(Rating.repo_id, count.label("num_events")) \
        .filter(Rating.created_at.between(start_date, end_date)) \
        .group_by(Rating.repo_id) \
        .order_by(count.desc())


def rank_repositories(repos, pos):
    """Add a rank to a list of repositories.

    :param repos: List of tuples.
    :param pos: Position in the tuple of `num_events`."""
    if not repos:
        return repos
    df = pd.DataFrame(repos)
    df['rank'] = df[pos].rank('min', ascending=False).astype(np.int)
    return df.to_records(index=False)
