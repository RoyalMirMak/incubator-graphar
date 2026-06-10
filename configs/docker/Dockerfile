ARG BASE_IMAGE=docker-hosted.artifactory.tcsbank.ru/rnd-grapher/graphar-dev-base-arrow-23-jdk-17
ARG BASE_IMAGE_TAG=v0.0.1
ARG IMAGE_TAG

FROM $BASE_IMAGE:$BASE_IMAGE_TAG AS build

USER root

RUN mkdir -p /storage/results \
    && chmod -R 755 /storage/results \
    && chown -R graphar:graphar /storage/results

ARG WORKDIR=/home/graphar

WORKDIR $WORKDIR

ENV PATH="$PATH:$WORKDIR/.local/bin/"

USER graphar

## Copy all sources
COPY . .

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN pip install -v ./cli

# graphar import -c /work/test.import.yml
ENTRYPOINT []
