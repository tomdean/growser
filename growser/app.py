from flask import Flask

from growser.services.bigquery import BigQueryService

app = Flask(__name__, template_folder="../templates")
app.config.from_object('growser.config.BasicConfig')
app.config.from_envvar('GROWSER_CONFIG', False)

with open(app.config.get('BIGQUERY_PRIVATE_KEY'), 'rb') as fh:
    key = fh.read()

bigquery = BigQueryService(app.config.get('BIGQUERY_PROJECT_ID'),
                           app.config.get('BIGQUERY_EMAIL'), key)
