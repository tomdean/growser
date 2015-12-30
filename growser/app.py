import os

from flask import Flask

from growser.services import bigquery, celery, database, log, storage


ROOT_PATH = os.path.realpath(__file__)

app = Flask(__name__, template_folder="../templates", static_folder="../static")
app.config.from_object('growser.config.BasicConfig')
app.config.from_envvar('GROWSER_CONFIG', False)

log = log.configure(app)
db = database.configure(app)
bigquery = bigquery.configure(app)
storage = storage.configure(app)
celery = celery.configure(app)
