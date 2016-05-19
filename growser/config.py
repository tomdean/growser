class DefaultConfig:
    """Configuration values that do not change based on the environment"""
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    #: Modules that Celery will use for finding tasks
    CELERY_INCLUDE = (
        'growser.tasks.github',
        'growser.tasks.rankings',
        'growser.tasks.screenshots',
        'growser.tasks.daily',
        'growser.tasks.commands'
    )

    #: Restart Celery workers after it has processed this number of tasks
    CELERYD_MAX_TASKS_PER_CHILD = 100

    CELERY_TASK_SERIALIZER = 'pickle'
    CELERY_RESULT_SERIALIZER = 'pickle'


class BasicConfig(DefaultConfig):
    """The configuration required to run Growser"""
    #: Enable debugging (logging, toolbar)
    DEBUG = True

    #: GitHub username & personal access token
    GITHUB_OAUTH = ('', '')

    #: Table Big Query will use for temporary storage during export
    BIG_QUERY_EXPORT_TABLE = "github.events_for_export"

    #: The Google Storage path used by Big Query for exporting events. Refer to
    #: https://goo.gl/dSWT6U for additional information.
    BIG_QUERY_EXPORT_PATH = "gs://my-bicket/events/events_{date}_*.csv.gz"

    #: Local path to download events
    LOCAL_IMPORT_PATH = "data/events"

    #: Google Cloud project to authenticate as
    GOOGLE_PROJECT_ID = ""

    #: Path to the JSON key exported from Google Console API credentials
    GOOGLE_CLIENT_KEY = "client_key.json"

    #: Database URL
    SQLALCHEMY_DATABASE_URI = "postgresql://user:pass@host:5432/growser"

    #: SQLAlchemy connection pool
    SQLALCHEMY_POOL_SIZE = 20

    #: Celery Broker
    BROKER_URL = ""

    #: Celery backend for persisting task results
    CELERY_BACKEND = ""
