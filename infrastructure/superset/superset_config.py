import os

ROW_LIMIT = 5000

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "J8u340f9hfn349h8f349h8f")

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DATABASE_URI",
    "postgresql://superset:password@postgres:5432/superset"
)

WTF_CSRF_ENABLED = True
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")

UPLOAD_FOLDER = "/app/superset_home/uploads/"
CSV_EXTENSIONS = {"csv", "tsv", "txt"}

ENABLE_PROXY_FIX = True