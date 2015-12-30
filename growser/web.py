from datetime import date, timedelta

from flask import abort, render_template, request
import pandas as pd

from growser import reports
from growser.api import blueprint
from growser.app import app
from growser.models import AllTimeRanking, AllTimeRankingByLanguage, Language, \
    MonthlyRanking, MonthlyRankingByLanguage, Repository, WeeklyRanking, \
    WeeklyRankingByLanguage


app.register_blueprint(blueprint, url_prefix="/api")


@app.route("/")
def index():
    """Interesting repositories."""
    return render_template("index.html")


@app.route("/r/<path:name>")
def repository(name: str):
    """Recommendations and other trends for a repository."""
    repo = Repository.query.filter(Repository.name == name).first_or_404()
    return render_template("repository.html", repo=repo)


@app.route("/r/<path:name>/rankings")
def rankings(name: str):
    repo = Repository.query.filter(Repository.name == name).first_or_404()
    return render_template("rankings.html", repo=repo)


@app.route("/o/<name>")
def organization(name: str):
    results = Repository.query.filter(Repository.owner == name) \
        .order_by(Repository.num_unique.desc()).all()
    return render_template("organization.html", results=results, org=name)


@app.route("/o/")
def organizations():
    results = reports.top_owners(100)
    return render_template("organizations.html", results=results)


@app.route("/b/")
@app.route("/b/<language>")
def browse(language: str=None):
    per_page = 100
    page = int(request.args.get('pp', 1))
    period = request.args.get('p', None)
    for_date = request.args.get('d', None)

    def week_start(fd):
        return pd.Period(fd, 'W-SAT')

    def month_start(fd):
        return pd.Period(fd, 'M')

    models = {None: [AllTimeRanking, AllTimeRankingByLanguage],
              'week': [WeeklyRanking, WeeklyRankingByLanguage, week_start],
              'month': [MonthlyRanking, MonthlyRankingByLanguage, month_start]}

    if period not in models:
        abort(404)

    opts = models[period]
    model = opts[1] if language else opts[0]
    query = model.query
    if language:
        query = query.filter(model.language == language)

    current_date = None
    if hasattr(model, 'date'):
        current_date = date.today()
        # Do not show rankings for current week
        if period == 'week':
            current_date = current_date - timedelta(days=7)

        def default_date():
            rv = date.today()
            if period == 'week':
                rv = date.today() - timedelta(days=7)
            return rv

        if not for_date:
            for_date = default_date()

        for_date = opts[2](for_date)
        query = query.filter(model.date == for_date.start_time.date())

    result = query.order_by(model.rank).limit(per_page) \
        .offset(0 if page == 1 else (page-1) * per_page + 1).all()

    languages = Language.top().to_dict()
    return render_template("language.html",
                           rankings=result, language=language, period=period,
                           for_date=for_date, current_date=current_date,
                           page=page, languages=languages)


if __name__ == "__main__":
    app.run(host='0.0.0.0')
