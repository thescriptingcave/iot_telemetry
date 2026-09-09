FROM apache/superset:latest

USER root

# 1) Create pip inside the venv (some Superset images ship without it)
RUN /app/.venv/bin/python -m ensurepip --upgrade

# 2) Upgrade pip/setuptools/wheel to avoid old build behavior
RUN /app/.venv/bin/python -m pip install --no-cache-dir --upgrade pip setuptools wheel

# 3) Install Trino SQLAlchemy dialect + Postgres driver (metadata DB)
RUN /app/.venv/bin/python -m pip install --no-cache-dir "trino[sqlalchemy]" "psycopg2-binary"

USER superset
