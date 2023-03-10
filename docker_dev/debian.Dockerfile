FROM debian:latest

# default to py3.11.1, this can be overridden at build, e.g. `docker build ... --build-arg version=3.10.8`
ARG version=3.11.1

# install python dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        zlib1g-dev \
        libncurses5-dev \
        libgdbm-dev \
        libnss3-dev \
        libssl-dev \
        libreadline-dev \
        libffi-dev \
        libsqlite3-dev \
        wget \
        libbz2-dev \
        git-all

# download, extract, and install python
RUN wget https://www.python.org/ftp/python/$version/Python-$version.tgz && \
    tar -xvf Python-$version.tgz && \
    cd Python-$version && \
    ./configure --enable-optimizations && \
    make -j $(shell nproc) && \
    make altinstall

# clean up
RUN apt-get clean && \
    rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /Python-$version.tgz

# add this installation to the path and update the default system interpreter to the newly installed version
RUN export PATH="/Python-$version:$PATH" && \
    update-alternatives --install /usr/bin/python3 python3 /Python-$version/python 1

# update python build tools
RUN python3 -m pip install --upgrade pip setuptools wheel --no-cache-dir

# setup mount for our code
WORKDIR /opt/code
VOLUME /opt/code

ENV PYTHONUNBUFFERED=1
