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


import signal
import threading
from time import sleep

from aet.kafka import KafkaConsumer

from . import config, healthcheck
from .logger import get_logger
from .workers import ESWorker

LOG = get_logger('MAIN')
CONSUMER_CONFIG = config.get_consumer_config()
KAFKA_CONFIG = config.get_kafka_config()

CONN_RETRY = int(CONSUMER_CONFIG.get('startup_connection_retry'))
CONN_RETRY_WAIT_TIME = int(CONSUMER_CONFIG.get('connect_retry_wait'))

MT = CONSUMER_CONFIG.get('multi-tenant', True)


class ESConsumerManager(object):

    def __init__(self):
        self.stopped = False
        self.autoconfigured_topics = []
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self.serve_healthcheck()
        self.groups = {}  # index_name : consumer group
        auto_conf = CONSUMER_CONFIG.get('autoconfig_settings', {})
        self.autoconf_maintainer = AutoConfMaintainer(self, auto_conf)
        self.autoconf_maintainer.start()

    def serve_healthcheck(self):
        self.healthcheck = healthcheck.HealthcheckServer()
        self.healthcheck.start()

    def notify_topic(self, tenant, topic):
        self.start_group(tenant)
        LOG.debug(f'Adding {topic} to tenant {tenant}')
        self.groups[tenant].add_topic(topic)

    def start_group(self, tenant):
        if not self.groups.get(tenant):
            LOG.debug(f'Adding new service for tenant: {tenant}')
            self.groups[tenant] = ESConsumerGroup(tenant)
            sleep(1)

    def stop_group(self, tenant):
        if self.groups.get(tenant):
            self.groups[tenant].stop()

    def stop(self, *args, **kwargs):
        self.stopped = True
        self.healthcheck.stop()
        for key in self.groups.keys():
            self.stop_group(key)


class AutoConfMaintainer(threading.Thread):

    def __init__(self, parent, autoconf=None):
        LOG.debug('Started Autoconf Maintainer')
        self.parent = parent
        self.autoconf = autoconf
        self.configured_topics = []
        self.consumer = KafkaConsumer(**KAFKA_CONFIG.copy())
        super(AutoConfMaintainer, self).__init__()

    def run(self):
        # On start
        while not self.parent.stopped:
            # Check autoconfig
            if not self.autoconf.get('enabled', False):
                LOG.debug('Autoconf disabled.')
            if self.autoconf.get('enabled', False):
                LOG.debug('Autoconfig checking for new indices')
                LOG.debug(
                    f'Previously Configured topics: {self.configured_topics}'
                )
                new_topics = self.find_new_topics()
                if not new_topics:
                    LOG.debug('No new topics')
                for topic in new_topics:
                    LOG.debug(
                        f'New topic: {topic}'
                    )
                    if MT:
                        parts = topic.split('.')
                        tenant = parts[0]
                    else:
                        tenant = CONSUMER_CONFIG.get('default_tenant')
                    self.parent.notify_topic(tenant, topic)
                    self.configured_topics.append(topic)

            # Check running threads
            try:
                self.check_running_groups()
            except Exception as err:
                LOG.error(f'Error watching running threads: {err}')
            for x in range(10):
                if self.parent.stopped:
                    return
                sleep(1)

    def find_new_topics(self):
        ignored_topics = set(self.autoconf.get('ignored_topics', []))
        try:
            md = self.consumer.list_topics()
            topics = [t.topic for t in md.topics.values()]
            topics = [i for i in topics
                      if i not in ignored_topics and
                      i not in self.configured_topics]
            return topics
        except Exception as ke:
            LOG.error('Autoconfig failed to get available topics \n%s' % (ke))
            return []  # Can't auto-configure if Kafka isn't available

    def check_running_groups(self):
        groups = self.parent.groups
        for k, group in groups.items():
            group.monitor_threads()


class ESConsumerGroup(object):
    # Consumer Group for a single tenant with a threaded consumer

    def __init__(self, tenant):
        LOG.debug(f'Consumer Group started for tenant: {tenant}')
        self.tenant = tenant
        self.topics = []  # configured topics
        self.worker = ESWorker(self.tenant)
        self.worker.start()

    def add_topic(self, topic):
        LOG.debug(f'Tenant {self.tenant} caught new topic {topic}')
        self.topics.append(topic)
        self.worker.add_topic(topic)

    def monitor_threads(self):
        LOG.debug(f'Checking liveness of tenant: {self.tenant}')
        if not self.worker.is_alive():
            LOG.error(f'Tenant {self.tenant} died. Restarting.')
            self.restart()

    def stop(self):
        self.worker.stop()

    def restart(self):
        self.worker.stop()
        self.worker.consumer.close(autocommit=True)
        self.worker = None
        self.worker = ESWorker(self.tenant)
        self.worker.start()
        sleep(2)
        self.worker.add_topics(self.topics)
