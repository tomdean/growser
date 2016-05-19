from datetime import date

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
