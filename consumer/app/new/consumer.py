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

from typing import Any, ClassVar, Dict, List

from aet.consumer import BaseConsumer
from aet.job import JobManager
from aet.api import APIServer
from aet.logger import get_logger

from aether.python.redis.task import TaskHelper


from . import artifacts

LOG = get_logger('MAIN')

# JOB_TYPE = artifacts.ESJob
# APIServer._allowed_types: ClassVar[Dict[str, List]] = {
#     _cls.name: _cls.public_actions for _cls in JOB_TYPE._resources
# }
# APIServer._allowed_types['job'] = [
#     'READ', 'CREATE', 'DELETE', 'LIST', 'VALIDATE',  # These are generic crud
#     'PAUSE', 'RESUME', 'STATUS'  # These are only valid for jobs
# ]

LOG.debug(f'Acceptable Types: {APIServer._allowed_types}')


class ElasticsearchConsumer(BaseConsumer):

    # # classes used by this consumer
    # _classes: ClassVar[Dict[str, Any]]

    # def __init__(self, CON_CONF, KAFKA_CONF, job_class=artifacts.ESJob, redis_instance=None):

    #     self.consumer_settings = CON_CONF
    #     self.kafka_settings = KAFKA_CONF
    #     if not redis_instance:
    #         redis_instance = type(self).get_redis(CON_CONF)
    #     self.task = TaskHelper(
    #         self.consumer_settings,
    #         redis_instance=redis_instance)
    #     self.job_manager = JobManager(self.task, job_class=JOB_TYPE)
    #     self._classes = {
    #         _cls.name: _cls for _cls in JOB_TYPE._resources
    #     }
    #     self._classes['job'] = JOB_TYPE
    #     self.serve_api(self.consumer_settings)

    def validate(self, job, _type=None, verbose=True, tenant=None):
        # consumer the tenant argument only because other methods need it
        _cls = self._classes.get(_type)
        if not _cls:
            return {'error': f'un-handled type: {_type}'}
        if verbose:
            return _cls._validate_pretty(job)
        else:
            return _cls._validate(job)
