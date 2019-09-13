#!/bin/bash
#
# Copyright (C) 2018 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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
#

source .env
cp .env aether-bootstrap/.env
pushd aether-bootstrap
source scripts/lib.sh
create_docker_assets
docker-compose -f elasticsearch/docker-compose.yml up -d kibana elasticsearch
sleep 10
docker-compose -f auth/docker-compose.yml run --rm gateway-manager add_elasticsearch_tenant dev 7
popd
docker-compose -f docker-compose-test.yml up elasticsearch-consumer-test-cloud