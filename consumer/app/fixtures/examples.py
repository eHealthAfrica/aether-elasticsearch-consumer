#!/usr/bin/env python

# Copyright (C) 2019 by eHealth Africa : http://www.eHealthAfrica.org
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

ES_INSTANCE = {
    'id': 'es-test',
    'name': 'Test ES Instance',
    'url': 'http://elasticsearch',
    'user': 'admin',
    'password': 'password'
}

# ES_INSTANCE = {
#     'id': 'es-test',
#     'name': 'Test ES Instance',
#     'url': 'https://alpha.eha.im/dev/elasticsearch/',
#     'user': 'user',
#     'password': 'password'
# }


KIBANA_INSTANCE = {
    'id': 'k-test',
    'name': 'Test Kibana Instance',
    'url': 'http://kibana',
    'user': 'admin',
    'password': 'password'
}

# KIBANA_INSTANCE = {
#     'id': 'k-test',
#     'name': 'Test Kibana Instance',
#     'url': 'https://alpha.eha.im/dev/kibana/kibana-app',
#     'user': 'user',
#     'password': 'password'
# }


LOCAL_ES_INSTANCE = {
    'id': 'default',
    'name': 'Test LOCAL ES Instance'
}

LOCAL_KIBANA_INSTANCE = {
    'id': 'default',
    'name': 'Test LOCAL Kibana Instance'
}


SUBSCRIPTION = {
    'id': 'sub-test',
    'name': 'Test Subscription',
    'topic_pattern': '*',
    'es_alias_name': 'test'
}

JOB = {
    'id': 'default',
    'name': 'Default ES Consumer Job',
    'local_kibana': 'default',
    'local_elasticsearch': 'default'
}

JOB_FOREIGN = {
    'id': 'j-test-foreign',
    'name': 'Default ES Consumer Job',
    'kibana': 'k-test',
    'elasticsearch': 'es-test',
    'subscriptions': ['sub-test']
}
