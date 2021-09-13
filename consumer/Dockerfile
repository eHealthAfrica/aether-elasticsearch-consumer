FROM python:3.8-slim-buster

LABEL description="Aether Elasticsearch Kafka consumer" \
      name="aether-elasticsearch-consumer" \
      author="eHealth Africa"

################################################################################
## install app
## copy files one by one and split commands to use docker cache
################################################################################

RUN apt-get update -qq > /dev/null && \
    apt-get -qq \
        --yes \
        --allow-downgrades \
        --allow-remove-essential \
        --allow-change-held-packages \
        install gcc > /dev/null && \
    useradd -ms /bin/false aether

WORKDIR /code
ENTRYPOINT ["/code/entrypoint.sh"]

COPY --chown=aether:aether ./conf/pip/requirements.txt /code/conf/pip/requirements.txt

ENV VIRTUAL_ENV=/var/run/aether/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN mkdir -p $VIRTUAL_ENV && \
    python3 -m venv $VIRTUAL_ENV && \
    pip install -q --upgrade pip && \
    pip install -q -r /code/conf/pip/requirements.txt

COPY --chown=aether:aether ./ /code

################################################################################
## last setup steps
################################################################################

ARG VERSION=0.0.0
ARG REVISION=alpha

RUN mkdir -p /var/tmp && \
    echo $VERSION > /var/tmp/VERSION && \
    echo $REVISION > /var/tmp/REVISION
