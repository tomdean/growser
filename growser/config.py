class DefaultConfig:
    SQLALCHEMY_TRACK_MODIFICATIONS = False


class BasicConfig(DefaultConfig):
    """The configuration required to run Growser"""

    #: GitHub username & personal access token
    GITHUB_OAUTH = ('', '')

    #: Table on Big Query to temporarily store events
    BIG_QUERY_EXPORT_TABLE = "github.events_for_export"

    #: The Google Storage path used by Big Query for exporting events
    BIG_QUERY_EXPORT_PATH = "gs://your_bucket_name/events/events_{date}_*.csv.gz"

    #: Local path to download events
    LOCAL_IMPORT_PATH = "data/events"

    #: Google Cloud project to authenticate as
    GOOGLE_PROJECT_ID = ""

    #: Path to the JSON key exported from Google Console API credentials
    GOOGLE_CLIENT_KEY = "/Users/tom/Projects/growser/client_key.json"

    #: Database URL
    SQLALCHEMY_DATABASE_URI = "postgresql://user:pass@host:5432/growser"

    SQLALCHEMY_POOL_SIZE = 20