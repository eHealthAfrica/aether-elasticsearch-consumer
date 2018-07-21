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
docker-compose -f docker-compose-test.yml build
sleep 5
docker-compose -f docker-compose-test.yml run assets-test register
docker-compose -f docker-compose-test.yml run assets-test generate 10
#sleep 10  # Wait for Kafka to finish coming up.
docker-compose -f docker-compose-test.yml run elasticsearch-consumer-test test_integration
pushd aether-bootstrap
#scripts/integration_test_teardown.sh
popd
