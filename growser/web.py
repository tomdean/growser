from datetime import date, timedelta
import locale
import re

from flask import render_template, request

from growser.app import app
from growser.models import Language, Ranking, RankingPeriod, Release, Repository


@app.route("/")
def index():
    """Interesting repositories."""
    return render_template("index.html")


@app.route("/r/<path:name>")
def repository(name: str):
    """Recommendations and other trends for a repository."""
    repo = Repository.query.filter(Repository.name == name).first_or_404()

    ranks = {'all': None, 'language': None}
    ranks['all'] = Ranking.query \
        .filter(Ranking.period == RankingPeriod.Recent) \
        .filter(Ranking.repo_id == repo.repo_id) \
        .filter(Ranking.language == "All") \
        .order_by(Ranking.start_date.desc()).first()

    if repo.language:
        ranks['language'] = Ranking.query \
            .filter(Ranking.period == RankingPeriod.Recent) \
            .filter(Ranking.repo_id == repo.repo_id) \
            .filter(Ranking.language == repo.language) \
            .order_by(Ranking.start_date.desc()).first()

    releases = Release.query \
        .filter(Release.repo_id == repo.repo_id) \
        .order_by(Release.created_at) \
        .all()

    from growser.models import Recommendation

    recommendations = Recommendation.find_by_repository(3, repo.repo_id, 100)

    return render("repository.html",
                  repo=repo,
                  releases=releases,
                  recommendations=recommendations,
                  ranks=ranks)


@app.route("/r/<path:name>/rankings")
def rankings(name: str):
    repo = Repository.query.filter(Repository.name == name).first_or_404()
    return render("rankings.html", repo=repo)


@app.route("/r/<path:name>/releases")
def repository_releases(name: str):
    repo = Repository.query.filter(Repository.name == name).first_or_404()
    return render("repository.releases.html", repo=repo)


@app.route("/developers")
def developers():
    return render("developers.html")


@app.route("/d/<path:name>")
def developer(name: str):
    return render("developer.html")


@app.route("/o/<name>")
def organization(name: str):
    results = Repository.query.filter(Repository.owner == name) \
        .order_by(Repository.num_unique.desc()).all()
    return render_template("organization.html", results=results, org=name)


@app.route("/o/")
def organizations():
    return render_template("organizations.html")


@app.route("/browse/")
@app.route("/browse/<language>")
def browse(language: str=None):
    page = int(request.args.get('pp', 1))
    period = request.args.get('p', None)
    for_date = request.args.get('d', None)
    language = language or "All"

    period_id = {None: 4, 'm': 2, 'w': 3, 'a': 1}.get(period)

    per_page = 100
    offset = 0 if page == 1 else (page-1) * per_page

    if not for_date:
        for_date = date.today() - timedelta(days=1)

    query = Ranking.query \
        .filter(Ranking.end_date == for_date) \
        .filter(Ranking.language == language) \
        .filter(Ranking.period == period_id) \
        .order_by(Ranking.rank)

    result = query.limit(per_page).offset(offset).all()
    return render("language.html", rankings=result, language=language,
                  period=period)


@app.route('/releases')
@app.route('/releases/<string:language>')
def releases(language: str=None):
    per_page = 100
    page = int(request.args.get('p', 1))
    offset = (page-1) * per_page

    languages = Language.top(15).all()
    results = Release.query.order_by(Release.created_at.desc())
    if language:
        results = results.filter(Release.repository.has(language=language))

    results = results.offset(offset).limit(per_page).all()

    return render("releases2.html",
                  language=language,
                  languages=languages,
                  releases=results,
                  page=page)


@app.route('/trending')
@app.route('/trending/<string:language>')
def trending(language: str=None):
    return render("trending.html", language=language)


def render(template, **kwargs):
    return render_template(template, ctx=kwargs)


@app.template_filter()
def ordinal(num):
    suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(num % 10, 'th')
    if 10 <= num % 100 <= 20:
        suffix = 'th'
    return str(num) + suffix


@app.template_filter()
def th(num):
    return locale.format('%d', num, 1)


@app.template_filter()
def emojis_to_img(txt):
    emojis = get_emojis()
    for emoji in re.findall(":([^:]+):", txt):
        if emoji not in emojis:
            continue
        txt = txt.replace(":{}:".format(emoji), '<img src="/static/{}" class="emoji" width="20" height="20" />'.format(emojis[emoji]))
    return txt


def get_colors():
    if 'colors' not in app.config:
        colors = open("data/github/languages.csv").readlines()
        app.config['colors'] = dict([l.strip().split(",") for l in colors])
    return app.config['colors']


def get_emojis():
    if 'emojis' not in app.config:
        emojis = open("data/github/emojis.csv").readlines()
        app.config['emojis'] = dict([l.strip().split(",") for l in emojis])
    return app.config['emojis']


if __name__ == "__main__":
    app.run(host='0.0.0.0')
