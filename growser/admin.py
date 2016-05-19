from flask import Blueprint, jsonify

from growser.app import app
from growser.services import commands
from growser.commands.media import UpdateRepositoryMedia

blueprint = Blueprint('admin', __name__)

commands = commands.configure(app)


@blueprint.route('/v1/<path:name>/update_media')
def update_media(name: str):
    cmd = UpdateRepositoryMedia(name)
    commands.execute(cmd)
    return jsonify({"status": "ok", "message": "Media Updated"})


@blueprint.route('/v1/<path:name>/update_github')
def update_github(name: str):
    pass
