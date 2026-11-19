FROM apache/airflow:3.4.0

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

USER root
COPY pyproject.toml /tmp/pyproject.toml
RUN uv pip install --python "$(command -v python)" --no-cache -r /tmp/pyproject.toml \
    && rm /tmp/pyproject.toml

USER airflow