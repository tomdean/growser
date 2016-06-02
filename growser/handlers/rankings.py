from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import func

from growser.app import db
from growser.cmdr import Handles, DomainEvent
from growser.commands.rankings import (
    UpdateRankings,
    UpdateAllTimeRankings,
    UpdateWeeklyRankings,
    UpdateMonthlyRankings,
    UpdateRecentRankings
)
from growser.db import from_sqlalchemy_table
from growser.models import Ranking, Rating, Repository


class RankingsUpdated(DomainEvent):
    def __init__(self, period, start_date, end_date, language):
        self.period = period
        self.start_date = start_date
        self.end_date = end_date
        self.language = language


class UpdateRankingsHandler(Handles[UpdateRankings]):
    def weekly(self, cmd: UpdateWeeklyRankings):
        """Set the time window for the rankings to the prior week."""
        if not cmd.end_date:
            cmd.end_date = date.today() - timedelta(days=1)
        cmd.start_date = cmd.end_date - timedelta(days=7)
        return self.handle(cmd)

    def monthly(self, cmd: UpdateMonthlyRankings):
        """Set the time window for the rankings to the prior month."""
        if not cmd.end_date:
            cmd.end_date = date.today() - timedelta(days=1)
        cmd.start_date = cmd.end_date - timedelta(days=30)
        return self.handle(cmd)

    def recent(self, cmd: UpdateRecentRankings):
        """Set the time window for the rankings to the prior 90 days."""
        if not cmd.end_date:
            cmd.end_date = date.today() - timedelta(days=1)
        cmd.start_date = cmd.end_date - timedelta(days=90)
        return self.handle(cmd)

    def alltime(self, cmd: UpdateAllTimeRankings):
        if not cmd.end_date:
            cmd.end_date = date.today() - timedelta(days=1)
        cmd.start_date = date(2012, 3, 1)
        return self.handle(cmd)

    def handle(self, cmd: UpdateRankings) -> RankingsUpdated:
        """Update project rankings"""
        if not cmd.language:
            raise ValueError("Language must not be empty")
        if cmd.start_date and cmd.start_date > cmd.end_date:
            raise ValueError("Invalid dates")

        rankings = self._query_database_num_ratings(cmd)

        # Use pandas to perform the ranking
        df = pd.DataFrame(rankings)
        df['language'] = cmd.language
        df['period'] = cmd.period
        df['start_date'] = cmd.start_date
        df['end_date'] = cmd.end_date
        df['rank'] = df[df.columns[1]].rank(0, 'min', ascending=False) \
            .astype(np.int)

        # Delete existing rankings prior to insert
        query = Ranking.query \
            .filter(Ranking.language == cmd.language) \
            .filter(Ranking.end_date == cmd.end_date) \
            .filter(Ranking.period == cmd.period)
        query.delete()

        data = iter(df.to_records(False))
        batch = from_sqlalchemy_table(Ranking.__table__, data, list(df.columns))
        batch.execute(db.engine.raw_connection)

        yield RankingsUpdated(
            cmd.period, cmd.start_date, cmd.end_date, cmd.language)

    @staticmethod
    def _query_database_num_ratings(cmd: UpdateRankings):
        """@todo Extract to an aggregate"""
        count = func.COUNT(Rating.repo_id)
        query = db.session.query(Rating.repo_id, count.label("num_events")) \
            .join(Repository, Repository.repo_id == Rating.repo_id) \
            .group_by(Rating.repo_id) \
            .order_by(count.desc())

        if cmd.language != "All":
            query = query.filter(Repository.language == cmd.language)
        if cmd.start_date:
            query = query.filter(Rating.created_at >= cmd.start_date)
        if cmd.end_date:
            query = query.filter(Rating.created_at < cmd.end_date)

        return query.limit(cmd.limit).all()
