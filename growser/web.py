import json

from flask import jsonify, render_template

from growser import reports
from growser.app import app
from growser.models import Repository
from growser.models import Recommendation, RecommendationModel


@app.route("/")
def index():
    """Interesting repositories."""
    return render_template("index.html")


@app.route("/r/<path:name>")
def repository(name: str):
    """Recommendations and other trends for a repository."""
    repo = Repository.query.filter(Repository.name == name).first_or_404()
    return render_template("repository.html", repo=repo)


@app.route("/r/<path:name>/timeline")
def timeline(name: str):
    return "test"


@app.route("/o/<name>")
def organization(name: str):
    results = Repository.query.filter(Repository.owner == name) \
        .order_by(Repository.num_unique.desc()).all()
    return render_template("organization.html", results=results, org=name)


@app.route("/o/")
def organizations():
    results = reports.top_owners(100)
    return render_template("organizations.html", results=results)


@app.route("/l/")
@app.route("/l/<name>")
def language(name: str=""):
    """Interesting repositories based on language."""
    query = Repository.query.order_by(Repository.num_unique.desc())
    if name:
        query = query.filter(Repository.language == name)
    results = query.limit(100).all()
    return render_template("language.html", results=results, language=name)


@app.route("/api/v1/daily_events/<repo_id>")
def report_daily_events(repo_id):
    daily_events = reports.daily_events_by_repo(repo_id)
    return jsonify({"result": daily_events})


@app.route("/api/v1/recommendations/<model_id>/<repo_id>")
def recommendations(model_id, repo_id):
    model = RecommendationModel.query.get_or_404(model_id)
    recs = Recommendation.query.filter(Recommendation.repo_id == repo_id).all()
    rv = {"model": model_to_dict(model), "results": []}
    for rec in recs:
        rv["results"].append({
            "score": rec.score,
            "repo": model_to_dict(rec.repository)
        })
    return jsonify(rv)


@app.template_filter('to_json')
def to_json(value):
    return json.dumps(value)


def model_to_dict(row):
    return dict((col, getattr(row, col))
                for col in row.__table__.columns.keys())

if __name__ == "__main__":
    app.run(host='0.0.0.0')
