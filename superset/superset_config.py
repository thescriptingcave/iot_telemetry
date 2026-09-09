import os

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}".format(
        user=os.environ.get("DATABASE_USER", "superset"),
        password=os.environ.get("DATABASE_PASSWORD", "superset"),
        host=os.environ.get("DATABASE_HOST", "superset-db"),
        port=os.environ.get("DATABASE_PORT", "5432"),
        db=os.environ.get("DATABASE_DB", "superset"),
    ),
)