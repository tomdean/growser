import calendar
from datetime import date, timedelta

from growser.cmdr import Command


class RankingPeriod:
    AllTime = 1
    Month = 2
    Week = 3
    Recent = 4
    Year = 5


class UpdateRankings(Command):
    def __init__(self, period: int, language: str, limit: int,
                 start_date: date=None, end_date: date=None):
        """Update the rankings for a collection of repositories.

        Example::

            UpdateRankings(3, "Java", 1000, date(2016, 5, 1), date(2016, 5, 7))

        :param period: Identifier for the period.
        :param language: Language to filter repositories to.
        :param limit: Number of repositories to include in the rankings.
        :param start_date: Earliest date to include user ratings (inclusive).
        :param end_date: Latest date to include user ratings (inclusive).
        """
        self.period = period
        self.language = language
        self.limit = limit
        self.start_date = start_date
        self.end_date = end_date

    def __repr__(self):
        return "{}(language={}, start_date={}, end_date={})".format(
            self.__class__.__name__, self.language,
            self.start_date, self.end_date)


class UpdateAllTimeRankings(UpdateRankings):
    def __init__(self, language, limit):
        """Update all-time rankings."""
        super().__init__(RankingPeriod.AllTime, language, limit)


class UpdateRecentRankings(UpdateRankings):
    def __init__(self, language, limit):
        """Update a 90-day rolling-window ranking of repositories.

        Example::

            UpdateRecentRankings("All", 1000)

        By default, :attr:`~UpdateRankings.end_date` is assumed to be yesterday.
        Historical rankings can be updated by overriding the default value::

            cmd = UpdateRecentRankings("All", 1000)
            cmd.end_date = date(2016, 5, 1)

        :param language: Language to limit ranked repositories to.
        :param limit: Number of repositories to include in the rankings.
        """
        super().__init__(RankingPeriod.Recent, language, limit)


class UpdateMonthlyRankings(UpdateRankings):
    def __init__(self, language, limit):
        """Update a 30-day rolling-window ranking of repositories.

        .. seealso:: :class:`UpdateRecentRankings`
        """
        super().__init__(RankingPeriod.Month, language, limit)


class UpdateWeeklyRankings(UpdateRankings):
    def __init__(self, language, limit):
        """Update a 7-day rolling-window ranking of repositories.

        .. seealso:: :class:`UpdateRecentRankings`
        """
        super().__init__(RankingPeriod.Week, language, limit)


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
