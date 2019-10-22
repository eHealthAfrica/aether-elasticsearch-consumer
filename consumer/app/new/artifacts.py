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
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json

from aet.job import BaseJob
from aet.logger import get_logger
from aet.resource import BaseResource, Draft7Validator, lock

from ..fixtures import schemas


LOG = get_logger('artifacts')


class ESInstance(BaseResource):
    schema = schemas.ES_INSTANCE
    jobs_path = '$.elasticsearch'
    name = 'elasticsearch'
    public_actions = BaseResource.public_actions


class KibanaInstance(BaseResource):
    schema = schemas.KIBANA_INSTANCE
    jobs_path = '$.kibana'
    name = 'kibana'
    public_actions = BaseResource.public_actions + [
        'poop'
    ]

    @lock
    def poop(self, object, reason):
        pass


class LocalESInstance(ESInstance):
    schema = schemas.LOCAL_ES_INSTANCE
    jobs_path = '$.local_elasticsearch'
    name = 'local_elasticsearch'
    public_actions = ESInstance.public_actions

    @classmethod
    def _validate(cls, definition) -> bool:
        # subclassing a Resource a second time breaks some of the class methods
        if cls.validator == LocalESInstance.validator:
            cls.validator = Draft7Validator(json.loads(cls.schema))
        return super(LocalESInstance, cls)._validate(definition)


class LocalKibanaInstance(KibanaInstance):
    schema = schemas.LOCAL_KIBANA_INSTANCE
    jobs_path = '$.local_kibana'
    name = 'local_kibana'
    public_actions = KibanaInstance.public_actions

    @classmethod
    def _validate(cls, definition) -> bool:
        # subclassing a Resource a second time breaks some of the class methods
        if cls.validator == KibanaInstance.validator:
            cls.validator = Draft7Validator(json.loads(cls.schema))
        return super(LocalKibanaInstance, cls)._validate(definition)


class Subscription(BaseResource):
    schema = schemas.SUBSCRIPTION
    jobs_path = '$.subscription'
    name = 'subscription'
    public_actions = BaseResource.public_actions + [
        'validate_pretty'
    ]


class ESJob(BaseJob):
    name = 'job'
    # Any type here needs to be registered in the API as APIServer._allowed_types
    _resources = [ESInstance, LocalESInstance, KibanaInstance, LocalKibanaInstance, Subscription]
    schema = schemas.ES_JOB
    # public_actions = ['READ', 'CREATE', 'DELETE', 'LIST', 'VALIDATE']
