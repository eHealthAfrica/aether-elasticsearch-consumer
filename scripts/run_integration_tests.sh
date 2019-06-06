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
set -Eeuo pipefail

pushd aether-bootstrap
scripts/integration_test_teardown.sh
scripts/integration_test_setup.sh
popd
docker-compose -f docker-compose-test.yml kill
docker-compose -f docker-compose-test.yml down
docker-compose -f docker-compose-test.yml build
docker-compose -f docker-compose-test.yml run -d elasticsearch-test-7
sleep 3
docker-compose -f docker-compose-test.yml run assets-test register
docker-compose -f docker-compose-test.yml run assets-test generate 10
echo Waiting for Kafka...
sleep 15  # Wait for Kafka to finish coming up.


echo "Elasticsearch 7.x Tests"
docker-compose -f docker-compose-test.yml run elasticsearch-consumer-test-7 test_integration
docker-compose -f docker-compose-test.yml kill
docker-compose -f docker-compose-test.yml down


echo "Elasticsearch 6.x Tests"
docker-compose -f docker-compose-test.yml run -d elasticsearch-test-6
sleep 15
docker-compose -f docker-compose-test.yml run elasticsearch-consumer-test-6 test_integration
docker-compose -f docker-compose-test.yml kill
docker-compose -f docker-compose-test.yml down

echo "Elasticsearch 5.x Tests"
docker-compose -f docker-compose-test.yml run -d elasticsearch-test-5
sleep 15
docker-compose -f docker-compose-test.yml run elasticsearch-consumer-test-5 test_integration
docker-compose -f docker-compose-test.yml kill
docker-compose -f docker-compose-test.yml down
pushd aether-bootstrap
scripts/integration_test_teardown.sh
popd
