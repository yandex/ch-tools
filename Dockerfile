ARG BASE_IMAGE=python:3.10.15-bookworm
FROM $BASE_IMAGE

ARG DEBIAN_FRONTEND=noninteractive

# Install poetry in a unified way
COPY . ./

RUN make install-poetry
RUN make lint
