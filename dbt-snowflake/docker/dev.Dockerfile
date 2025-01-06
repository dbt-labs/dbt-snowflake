# this image does not get published, it is intended for local development only, see `Makefile` for usage
FROM ubuntu:24.04 AS base

# prevent python installation from asking for time zone region
ARG DEBIAN_FRONTEND=noninteractive

# add python repository
RUN apt-get update \
    && apt-get install -y software-properties-common=0.99.48 \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/*

# install python
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential=12.10ubuntu1 \
        git-all=1:2.43.0-1ubuntu7.1 \
        python3.9=3.9.20-1+noble1 \
        python3.9-dev=3.9.20-1+noble1 \
        python3.9-distutils=3.9.20-1+noble1 \
        python3.9-venv=3.9.20-1+noble1 \
        python3-pip=24.0+dfsg-1ubuntu1 \
        python3-wheel=0.42.0-2 \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \

# update the default system interpreter to the newly installed version
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.9 1


FROM base AS dbt-snowflake-dev

HEALTHCHECK CMD python --version || exit 1

# send stdout/stderr to terminal
ENV PYTHONUNBUFFERED=1

# setup mount for local code
WORKDIR /opt/code
VOLUME /opt/code

# create a virtual environment
RUN python -m venv /opt/venv
