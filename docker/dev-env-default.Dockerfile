FROM docker/dev-environments-default:latest

# install python and git (for installing dbt-core)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-pip \
        python3-wheel \
        build-essential

# clean up
RUN apt-get clean && \
    rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /Python-3.8.15.tgz

# update python build tools
RUN python3 -m pip install --upgrade pip setuptools wheel --no-cache-dir

# setup mount for our code
WORKDIR /opt/code
VOLUME /opt/code

# send stdout/stderr to terminal
ENV PYTHONUNBUFFERED=1
