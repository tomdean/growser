import json
import logging

from flask import Flask

from growser.services.bigquery import BigQueryService


app = Flask(__name__, template_folder="../templates", static_folder="../static")
app.config.from_object('growser.config.BasicConfig')
app.config.from_envvar('GROWSER_CONFIG', False)
app.debug = True

with open(app.config.get('GOOGLE_CLIENT_KEY')) as fh:
    js = json.loads(fh.read())

bigquery = BigQueryService(app.config.get('GOOGLE_PROJECT_ID'),
                           js['client_email'],
                           bytes(js['private_key'], 'UTF-8'))
