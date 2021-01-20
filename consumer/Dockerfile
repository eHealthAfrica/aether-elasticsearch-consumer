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
    pip install -q --upgrade pip

WORKDIR /code

COPY ./conf/pip/requirements.txt /code/conf/pip/requirements.txt
RUN pip install -q -r /code/conf/pip/requirements.txt

COPY ./ /code

################################################################################
## last setup steps
################################################################################

# create user to run container (avoid root user)
RUN useradd -ms /bin/false aether
RUN chown -R aether: /code

ARG VERSION=0.0.0
ARG REVISION=alpha

RUN mkdir -p /var/tmp && \
    echo $VERSION > /var/tmp/VERSION && \
    echo $REVISION > /var/tmp/REVISION

ENTRYPOINT ["/code/entrypoint.sh"]
