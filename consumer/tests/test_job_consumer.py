#!/usr/bin/env python

# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import pytest
import requests
import json
from time import sleep

from . import *  # noqa
from . import (  # noqa  # for the linter
    ElasticsearchConsumer,
    RequestClientT1,
    RequestClientT2,
    URL,
    check_local_es_readyness
)

from aet.logger import get_logger
from app.fixtures import examples

LOG = get_logger('TEST')


'''
    API Tests
'''


@pytest.mark.v2
def test__consumer_add_delete_respect_tenants(ElasticsearchConsumer, RequestClientT1, RequestClientT2):
    res = RequestClientT1.post(f'{URL}/elasticsearch/add', json=examples.ES_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/elasticsearch/list')
    assert(res.json() != [])
    res = RequestClientT2.get(f'{URL}/elasticsearch/list')
    assert(res.json() == [])
    res = RequestClientT1.delete(f'{URL}/elasticsearch/delete?id=es-test')
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/elasticsearch/list')
    assert(res.json() == [])


@pytest.mark.parametrize('example,endpoint', [
    (examples.ES_INSTANCE, 'elasticsearch'),
    (examples.KIBANA_INSTANCE, 'kibana'),
    (examples.LOCAL_ES_INSTANCE, 'local_elasticsearch'),
    (examples.LOCAL_KIBANA_INSTANCE, 'local_kibana'),
    (examples.JOB, 'job')
])
@pytest.mark.v2
def test__api_validate(ElasticsearchConsumer, RequestClientT1, example, endpoint):
    res = RequestClientT1.post(f'{URL}/{endpoint}/validate', json=example)
    assert(res.json().get('valid') is True), str(res.text)


@pytest.mark.parametrize('example,endpoint', [
    (examples.ES_INSTANCE, 'elasticsearch'),
    (examples.KIBANA_INSTANCE, 'kibana'),
    (examples.LOCAL_ES_INSTANCE, 'local_elasticsearch'),
    (examples.LOCAL_KIBANA_INSTANCE, 'local_kibana'),
    (examples.JOB, 'job')
])
@pytest.mark.v2
def test__api_validate_pretty(ElasticsearchConsumer, RequestClientT1, example, endpoint):
    res = RequestClientT1.post(f'{URL}/{endpoint}/validate_pretty', json=example)
    assert(res.json().get('valid') is True), str(res.text)


@pytest.mark.parametrize('endpoint', [
    ('elasticsearch'),
    ('kibana'),
    ('local_elasticsearch'),
    ('local_kibana'),
    ('job')
])
@pytest.mark.v2
def test__api_describe_assets(ElasticsearchConsumer, RequestClientT1, endpoint):
    res = RequestClientT1.get(f'{URL}/{endpoint}/describe')
    assert(res.json() is not None), str(res.text)


@pytest.mark.parametrize('endpoint', [
    ('elasticsearch'),
    ('kibana'),
    ('local_elasticsearch'),
    ('local_kibana'),
    ('job')
])
@pytest.mark.v2
def test__api_get_schema(ElasticsearchConsumer, RequestClientT1, endpoint):
    res = RequestClientT1.get(f'{URL}/{endpoint}/get_schema')
    assert(res.json() is not None), str(res.text)


@pytest.mark.v2
def test__api_resource_instance(ElasticsearchConsumer, RequestClientT1, RequestClientT2):
    doc_id = examples.KIBANA_INSTANCE.get("id")
    res = RequestClientT1.post(f'{URL}/kibana/add', json=examples.KIBANA_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/kibana/list')
    assert(doc_id in res.json())
    res = RequestClientT1.get(f'{URL}/kibana/test_connection?id={doc_id}')
    try:
        res.raise_for_status()
    except requests.HTTPError as her:
        assert(res.status_code == 500), (her, res.content)
    else:
        assert(False), 'Asset should be in-accessible as it does not exist'
    res = RequestClientT2.get(f'{URL}/kibana/test_connection?id={doc_id}')
    try:
        res.raise_for_status()
    except requests.HTTPError as her:
        assert(res.status_code == 404), (her, res.content)
    else:
        assert(False), 'Asset should be missing for this tenant'
    res = RequestClientT1.delete(f'{URL}/kibana/delete?id={examples.KIBANA_INSTANCE.get("id")}')
    assert(res.json() is True)


@pytest.mark.v2
def test__api_resource_es(ElasticsearchConsumer, RequestClientT1):
    doc_id = examples.ES_INSTANCE.get("id")
    res = RequestClientT1.post(f'{URL}/elasticsearch/add', json=examples.ES_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/elasticsearch/list')
    assert(doc_id in res.json())
    res = RequestClientT1.get(f'{URL}/elasticsearch/test_connection?id={doc_id}')
    try:
        res.raise_for_status()
    except requests.HTTPError:
        assert(res.status_code == 500)


@pytest.mark.v2_integration
def test__api_resource_es_local(ElasticsearchConsumer, RequestClientT1):
    doc_id = examples.LOCAL_ES_INSTANCE.get("id")
    res = RequestClientT1.post(f'{URL}/local_elasticsearch/add', json=examples.LOCAL_ES_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/local_elasticsearch/list')
    assert(doc_id in res.json())
    res = RequestClientT1.get(f'{URL}/local_elasticsearch/test_connection?id={doc_id}')
    assert(res.json().get('cluster_name') is not None)


@pytest.mark.v2_integration
def test__api_resource_es_foreign(ElasticsearchConsumer, RequestClientT1):
    doc_id = examples.ES_INSTANCE.get("id")
    res = RequestClientT1.post(f'{URL}/elasticsearch/add', json=examples.ES_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/elasticsearch/list')
    assert(doc_id in res.json())
    res = RequestClientT1.get(f'{URL}/elasticsearch/test_connection?id={doc_id}')
    res.raise_for_status()
    assert(res.json().get('cluster_name') is not None)


@pytest.mark.v2_integration
def test__api_resource_kibana_local(ElasticsearchConsumer, RequestClientT1):
    doc_id = examples.LOCAL_KIBANA_INSTANCE.get("id")
    res = RequestClientT1.post(f'{URL}/local_kibana/add', json=examples.LOCAL_KIBANA_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/local_kibana/list')
    assert(doc_id in res.json())
    res = RequestClientT1.get(f'{URL}/local_kibana/test_connection?id={doc_id}')
    res.raise_for_status()
    assert(res.content is not None), res.content


@pytest.mark.v2_integration
def test__api_resource_kibana_foreign(ElasticsearchConsumer, RequestClientT1):
    doc_id = examples.KIBANA_INSTANCE.get("id")
    res = RequestClientT1.post(f'{URL}/kibana/add', json=examples.KIBANA_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.get(f'{URL}/kibana/list')
    assert(doc_id in res.json())
    res = RequestClientT1.get(f'{URL}/kibana/test_connection?id={doc_id}')
    try:
        res.raise_for_status()
    except Exception:
        LOG.error([res.status_code, res.content])
    assert(res.content is not None), res.content


@pytest.mark.v2_integration
def test__api_job_and_resource_create(ElasticsearchConsumer, RequestClientT1):
    doc_id = examples.JOB_FOREIGN.get("id")
    res = RequestClientT1.post(f'{URL}/kibana/add', json=examples.KIBANA_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.post(f'{URL}/elasticsearch/add', json=examples.ES_INSTANCE)
    assert(res.json() is True)
    res = RequestClientT1.post(f'{URL}/subscription/add', json=examples.SUBSCRIPTION)
    assert(res.json() is True)

    res = RequestClientT1.post(f'{URL}/job/add', json=examples.JOB_FOREIGN)
    assert(res.json() is True)

    sleep(.25)  # take a few MS for the job to be started


@pytest.mark.v2_integration
def test__api_job_and_resource_public_endpoints(ElasticsearchConsumer, RequestClientT1):
    doc_id = examples.JOB_FOREIGN.get("id")
    res = RequestClientT1.get(f'{URL}/job/list_topics?id={doc_id}')
    LOG.debug(res.content)
    res.raise_for_status()
    topics = res.json()
    assert(len(topics) == 1 and TEST_TOPIC in topics)
    res = RequestClientT1.get(f'{URL}/job/list_subscribed_topics?id={doc_id}')
    res.raise_for_status()
    topics = res.json()
    LOG.debug(topics)
    assert(TEST_TOPIC not in topics)
    sleep(60)
    res = RequestClientT1.get(f'{URL}/job/get_logs?id={doc_id}')
    res.raise_for_status()
    logs = res.json()
    assert(len(logs) > 0)


@pytest.mark.v2_integration
def test__api_job_and_resource_delete(ElasticsearchConsumer, RequestClientT1):
    doc_id = examples.JOB_FOREIGN.get("id")
    res = RequestClientT1.delete(f'{URL}/kibana/delete?id={examples.KIBANA_INSTANCE.get("id")}')
    assert(res.json() is True)
    res = RequestClientT1.delete(f'{URL}/elasticsearch/delete?id={examples.ES_INSTANCE.get("id")}')
    assert(res.json() is True)
    res = RequestClientT1.post(f'{URL}/subscription/delete?id={examples.SUBSCRIPTION.get("id")}')
    assert(res.json() is True)

    res = RequestClientT1.post(f'{URL}/job/delete?id={doc_id}', json=examples.JOB_FOREIGN)
    assert(res.json() is True)
