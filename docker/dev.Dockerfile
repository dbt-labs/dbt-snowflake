# this image does not get published, it is intended for local development only, see `Makefile` for usage
FROM ubuntu:22.04 as base

ARG py_version=3.11

# prevent python installation from asking for time zone region
ARG DEBIAN_FRONTEND=noninteractive

# add python repository
RUN apt-get update \
  && apt-get install -y software-properties-common=0.99.22.9 \
  && add-apt-repository -y ppa:deadsnakes/ppa \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# install python
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential=12.9ubuntu3 \
    git-all=1:2.34.1-1ubuntu1.10 \
    python$py_version \
    python$py_version-dev \
    python$py_version-distutils \
    python$py_version-venv \
    python3-pip=22.0.2+dfsg-1ubuntu0.4 \
    python3-wheel=0.37.1-2ubuntu0.22.04.1 \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

# update the default system interpreter to the newly installed version
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python$py_version 1


FROM base as dbt-snowflake-dev

HEALTHCHECK CMD python3 --version || exit 1

# send stdout/stderr to terminal
ENV PYTHONUNBUFFERED=1

# setup mount for local code
WORKDIR /opt/code
VOLUME /opt/code

# create a virtual environment
RUN python3 -m venv /opt/venv
