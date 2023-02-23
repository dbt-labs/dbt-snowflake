FROM ubuntu:latest

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
        python3.8 \
        python3.8-dev \
        python3.8-venv \
        python3.8-distutils \
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
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1

# update python build tools
RUN python3 -m pip install --upgrade pip setuptools wheel --no-cache-dir

# setup mount for our code
WORKDIR /opt/code
VOLUME /opt/code

# send stdout/stderr to terminal
ENV PYTHONUNBUFFERED=1
