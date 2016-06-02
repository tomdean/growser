from typing import List

from growser.cmdr import handles

from datetime import date, timedelta
from growser.models import Ranking, Recommendation, Repository
from growser.queries import (
    FindProject,
    FindCurrentRankings,
    FindRecommendations
)


@handles(FindProject)
def find_project(query: FindProject) -> Repository:
    return Repository.query.filter_by(name=query.name).first()


@handles(FindCurrentRankings)
def find_rankings(query: FindCurrentRankings) -> List[Ranking]:
    end_date = date.today() - timedelta(days=1)
    query = Ranking.query \
        .filter(Ranking.repo_id == query.repo_id) \
        .filter(Ranking.end_date == end_date)

    rv = []
    for r in query.all():
        print(r.period, r.end_date, r.language)
        rv.append(r)

    return rv


@handles(FindRecommendations)
def find_recommendations(query: FindRecommendations) -> List[Recommendation]:
    query = Recommendation.find_by_repository(
        query.model, query.repo_id, query.limit)
    return query
