FROM debian:latest

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
RUN wget https://www.python.org/ftp/python/3.8.15/Python-3.8.15.tgz && \
    tar -xvf Python-3.8.15.tgz && \
    cd Python-3.8.15 && \
    ./configure --enable-optimizations && \
    make -j $(shell nproc) && \
    make altinstall

# clean up
RUN apt-get clean && \
    rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /Python-3.8.15.tgz

# add this installation to the path and update the default system interpreter to the newly installed version
RUN export PATH="/Python-3.8.15:$PATH" && \
    update-alternatives --install /usr/bin/python3 python3 /Python-3.8.15/python 1

# update python build tools
RUN python3 -m pip install --upgrade pip setuptools wheel --no-cache-dir

# setup mount for our code
WORKDIR /opt/code
VOLUME /opt/code

ENV PYTHONUNBUFFERED=1
