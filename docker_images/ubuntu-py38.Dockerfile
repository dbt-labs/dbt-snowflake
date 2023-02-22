FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y software-properties-common && \
    apt-get update && add-apt-repository -y ppa:deadsnakes/ppa && \
    apt-get update && apt-get install --no-install-recommends -y python3.8 python3.8-dev \
        python3.8-venv python3.8-distutils python3-pip python3-wheel build-essential && \
    apt-get update && apt-get install -y git-all && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1

RUN mkdir /opt/code
VOLUME /opt/code

EXPOSE 5000

ENV PYTHONUNBUFFERED=1
