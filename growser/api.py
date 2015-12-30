from flask import Blueprint, jsonify

from growser import reports
from growser.models import Recommendation, RecommendationModel

blueprint = Blueprint('api', __name__)


@blueprint.route("/v1/events/<repo_id>")
def events(repo_id):
    return jsonify(result=reports.daily_events_by_repo(repo_id))


@blueprint.route("/v1/recommendations/<model_id>/<repo_id>")
def recommendations(model_id, repo_id):
    model = RecommendationModel.query.get_or_404(model_id)
    recs = Recommendation.query \
        .filter(Recommendation.model_id == model_id) \
        .filter(Recommendation.repo_id == repo_id)\
        .order_by(Recommendation.score.desc()).all()

    rv = {"model": model_to_dict(model), "results": []}
    for rec in recs:
        repo = model_to_dict(rec.repository)
        repo['url'] = "/r/" + repo['name']
        repo['github_url'] = "https://github.com/" + repo['name']
        repo['image'] = "/static/github/t/" + rec.repository.hashid + ".jpg"
        rv["results"].append({
            "score": rec.score,
            "repo": repo
        })
    return jsonify(rv)


def model_to_dict(row):
    return dict((col, getattr(row, col))
                for col in row.__table__.columns.keys())
