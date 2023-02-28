FROM ubuntu:latest

# default to py3.11, this can be overridden at build, e.g. `docker build ... --build-arg version=3.10`
ARG version=3.11

# prevent python installation from asking for time zone region
ARG DEBIAN_FRONTEND=noninteractive

# get add-apt-repository
RUN apt-get update && \
    apt-get install -y software-properties-common

# add the python repository
RUN apt-get update && \
    add-apt-repository -y ppa:deadsnakes/ppa

# install python and git (for installing dbt-core)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python$version \
        python$version-dev \
        python$version-distutils \
        python$version-venv \
        python3-pip \
        python3-wheel \
        build-essential \
        git-all

# clean up
RUN apt-get clean && \
    rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/*

# update the default system interpreter to the newly installed version
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python$version 1

# setup mount for our code
WORKDIR /opt/code
VOLUME /opt/code

# install tox in the system interpreter (it creates it's own virtual environments)
RUN pip install tox

# explicitly create a virtual environment as well for interactive testing
RUN python3 -m venv /opt/venv

# send stdout/stderr to terminal
ENV PYTHONUNBUFFERED=1
