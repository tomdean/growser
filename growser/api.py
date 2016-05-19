import os
import re

from flask import Blueprint, jsonify
import markdown

from growser.models import (Recommendation, RecommendationModel, Release)

blueprint = Blueprint('api', __name__)


@blueprint.route("/v1/recommendations/<model_id>/<repo_id>")
def recommendations(model_id, repo_id):
    model = RecommendationModel.query.get_or_404(model_id)
    recs = Recommendation.find_by_repository(model_id, repo_id)

    rv = {"model": model.to_dict(), "results": []}
    for rec in recs:
        repo = rec.repository.to_dict()
        repo['url'] = "/r/" + repo['name']
        repo['github_url'] = "https://github.com/" + repo['name']
        repo['image'] = "/static/github/ts/" + rec.repository.hashid + ".hp.png"

        # Temp hack
        if not os.path.exists(repo['image'][1:]):
            repo['image'] = repo['image'].replace(".hp.", ".readme.")

        rv["results"].append({"score": rec.score, "repo": repo})

    return jsonify(rv)


@blueprint.route('/v1/release/<int:release_id>')
def release(release_id):
    result = Release.query.get(release_id)
    body = re.sub(r'(?<!\[)#(\d+)', r'[#\1](https://github.com/{0}/issues/\1)'
                  .format(result.repository.name), result.body)
    return markdown.markdown(body, extensions=[AutolinkExtension()])


class AutolinkPattern(markdown.inlinepatterns.Pattern):
    def handleMatch(self, m):
        el = markdown.util.etree.Element("a")

        href = m.group(2)
        if not re.match('^(ftp|https?)://', href, flags=re.IGNORECASE):
            href = 'http://%s' % href
        el.set('href', self.unescape(href))

        el.text = markdown.util.AtomicString(m.group(2))
        return el


class AutolinkExtension(markdown.Extension):
    def extendMarkdown(self, md, md_globals):
        url_re = r'(?i)\b((?:(?:ftp|https?)://|www\d{0,3}[.])(?:[^\s()<>]+|' + \
            r'\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()' \
            + r'<>]+\)))*\)|[^\s`!()\[\]{};:' + r"'" + r'".,<>?«»“”‘’]))'
        autolink = AutolinkPattern(url_re, md)
        md.inlinePatterns.add('gfm-autolink', autolink, '_end')
