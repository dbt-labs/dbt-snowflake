ARG build_for=linux/amd64
ARG py_version=3.10.7
ARG dbt_snowflake_ref=dbt-snowflake@main


FROM --platform=$build_for python:$py_version-slim-bullseye as base

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir


# this image gets published to GHCR for production use
FROM base as dbt-snowflake

WORKDIR /usr/app/dbt/
ENTRYPOINT ["dbt"]

RUN python -m pip install --no-cache-dir "dbt-snowflake @ git+https://github.com/dbt-labs/${dbt_snowflake_ref}"

HEALTHCHECK CMD dbt --version || exit 1


# this image does not get published, it is intended for local development only, see `Makefile` for usage
FROM base as dbt-snowflake-dev

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
        git-all \
        libbz2-dev \
        libffi-dev \
        libgdbm-dev \
        libncurses5-dev \
        libnss3-dev \
        libreadline-dev \
        libsqlite3-dev \
        libssl-dev \
        wget \
        zlib1g-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# setup mount for local code
WORKDIR /opt/code
VOLUME /opt/code

ENTRYPOINT ["python", "-m", "pip", "install", "--no-cache-dir", "-e", ".", "-r dev-requirements.txt", "&&", "pre-commit install"]

HEALTHCHECK CMD dbt --version || exit 1

ENV PYTHONUNBUFFERED=1
