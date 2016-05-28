import locale
import os

from flask import Flask

from growser.services import bigquery, celery, sqlalchemy, log, storage


ROOT_PATH = os.path.realpath(__file__)

locale.setlocale(locale.LC_ALL, '')


app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.config.from_object('growser.config.BasicConfig')
app.config.from_envvar('GROWSER_CONFIG', False)

log = log(app, 'logging.cfg')
bigquery = bigquery(app)
storage = storage(app)
celery = celery(app)
db = sqlalchemy(app)
