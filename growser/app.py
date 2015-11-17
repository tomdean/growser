import logging

from flask import Flask

from growser.services.bigquery import BigQueryService


app = Flask(__name__, template_folder="../templates", static_folder="../static")
app.config.from_object('growser.config.BasicConfig')
app.config.from_envvar('GROWSER_CONFIG', False)
app.debug = True

with open(app.config.get('BIGQUERY_PRIVATE_KEY'), 'rb') as fh:
    key = fh.read()

bigquery = BigQueryService(app.config.get('BIGQUERY_PROJECT_ID'),
                           app.config.get('BIGQUERY_EMAIL'), key)
