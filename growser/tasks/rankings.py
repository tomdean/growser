import calendar
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import func
from sqlalchemy.sql import delete

from growser.app import app, celery, db
from growser.models import AllTimeRanking, AllTimeRankingByLanguage, \
    MonthlyRanking, MonthlyRankingByLanguage, Rating, Repository, \
    WeeklyRanking, WeeklyRankingByLanguage
from growser.tasks.database import BulkInsertList

#: Earliest date to generate rankings for
INITIAL_DATE = date(2012, 4, 1)

#: Number of repositories to include in each ranking
RANK_TOP_N = 1000


@celery.task(ignore_result=True)
def update_alltime_rankings():
    """Update all-time rankings."""
    query = get_top_repositories().limit(RANK_TOP_N).all()
    ranked = rank_repositories(list(query), 1)

    AllTimeRanking.query.delete()

    columns = ["repo_id", "num_events", "rank"]
    batch = BulkInsertList(AllTimeRanking.__table__, ranked, columns)
    batch.execute(db.engine)


@celery.task(ignore_result=True)
def update_alltime_language_rankings(language: str):
    """Update all-time rankings for a single language.

    .. seealso:: :func:`update_all_alltime_language_rankings`
    """
    app.logger.info("Updating all-time rankings for %s", language)
    query = get_top_repositories() \
        .join(Repository, Repository.repo_id == Rating.repo_id) \
        .filter(Repository.language == language) \
        .limit(RANK_TOP_N)

    repos = map(lambda x: (language,) + x, query.all())
    ranked = rank_repositories(list(repos), 2)

    AllTimeRankingByLanguage.query.filter(
        AllTimeRankingByLanguage.language == language).delete()

    columns = ["language", "repo_id", "num_events", "rank"]
    batch = BulkInsertList(AllTimeRankingByLanguage.__table__, ranked, columns)
    batch.execute(db.engine)


@celery.task(ignore_result=True)
def update_weekly_rankings(start_date: date):
    """Update rankings for the week beginning on `start_date`."""
    start_date = to_first_day_of_week(start_date)
    end_date = start_date + timedelta(days=7)
    _update_rankings(WeeklyRanking.__table__, start_date, end_date)


@celery.task(ignore_result=True)
def update_monthly_rankings(start_date: date):
    """Update rankings for the month of `start_date`."""
    start_date = start_date.replace(day=1)
    days = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = start_date + timedelta(days=days)
    _update_rankings(MonthlyRanking.__table__, start_date, end_date)


@celery.task(ignore_result=True)
def update_weekly_language_rankings(language: str, start_date: date):
    """Update language rankings for the week beginning on `start_date`."""
    start_date = to_first_day_of_week(start_date)
    end_date = start_date + timedelta(days=7)
    _update_language_rankings(WeeklyRankingByLanguage.__table__,
                              language, start_date, end_date)


@celery.task(ignore_result=True)
def update_monthly_language_rankings(language: str, start_date: date):
    """Update language rankings for the month of `start_date`."""
    start_date = start_date.replace(day=1)
    days = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = start_date + timedelta(days=days)
    _update_language_rankings(MonthlyRankingByLanguage.__table__,
                              language, start_date, end_date)


@celery.task
def update_all_alltime_language_rankings():
    """Update All-Time Rankings for all languages."""
    for language in get_top_languages():
        update_alltime_language_rankings.delay(language)


@celery.task
def update_all_weekly_rankings():
    """Update all weekly rankings we have data for."""
    for week in weekly_interval(INITIAL_DATE, date.today()):
        update_weekly_rankings.delay(week)


@celery.task
def update_all_weekly_language_rankings():
    """Update weekly rankings for all languages and weeks."""
    for language in get_top_languages():
        for week in weekly_interval(INITIAL_DATE, date.today()):
            update_weekly_language_rankings.delay(language, week)


@celery.task
def update_all_monthly_rankings():
    """Update all monthly rankings we have data for."""
    for month in monthly_interval(INITIAL_DATE, date.today()):
        update_monthly_rankings.delay(month)


@celery.task
def update_all_monthly_language_rankings():
    """Update monthly rankings for all languages and months."""
    for language in get_top_languages():
        for month in monthly_interval(INITIAL_DATE, date.today()):
            update_monthly_language_rankings.delay(language, month)


def _update_rankings(table, start_date: date, end_date: date):
    app.logger.info("Updating %s for %s", table, start_date)
    query = get_top_repositories_by_date(start_date, end_date).limit(RANK_TOP_N)

    for_date = start_date.strftime("%Y-%m-%d")
    repos = map(lambda x: (for_date,) + x, query.all())
    ranked = rank_repositories(list(repos), 2)

    db.engine.execute(delete(table).where(table.c.date == start_date))

    columns = ["date", "repo_id", "num_events", "rank"]
    batch = BulkInsertList(table, ranked, columns)
    batch.execute(db.engine)


def _update_language_rankings(table, language: str,
                              start_date: date, end_date: date):
    app.logger.info("Updating %s for %s (%s)", table, start_date, language)
    query = get_top_repositories_by_date(start_date, end_date) \
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


def get_top_repositories_by_date(start_date: date, end_date: date):
    """Most popular repositories by number of stars + forks.

    :param start_date: Include ratings since this date.
    :param end_date: Include ratings until this date."""
    return get_top_repositories() \
        .filter(Rating.created_at >= start_date) \
        .filter(Rating.created_at < end_date)


def get_top_repositories():
    count = func.COUNT(Rating.repo_id)
    return db.session.query(Rating.repo_id, count.label("num_events")) \
        .group_by(Rating.repo_id) \
        .order_by(count.desc())


def rank_repositories(repos: list, pos: int) -> list:
    """Add a rank to a list of repositories.

    :param repos: List of tuples.
    :param pos: Position in the tuple of `num_events`."""
    if not repos:
        return repos
    df = pd.DataFrame(repos)
    df['rank'] = df[pos].rank('min', ascending=False).astype(np.int)
    return df.to_records(index=False)


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
