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


import json
import pytest
from . import *  # noqa

from app.fixtures import examples

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
def test__validate_example_assets(ElasticsearchConsumer, RequestClientT1, example, endpoint):
    res = RequestClientT1.post(f'{URL}/{endpoint}/validate', json=example)
    assert(res.json().get('valid') is True)
