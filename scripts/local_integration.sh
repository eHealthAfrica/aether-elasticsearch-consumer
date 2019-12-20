docker-compose -f ./docker-compose-test.yml up -d elasticsearch kibana
docker-compose -f ./docker-compose-test.yml run --rm consumer-test test_integration
# docker-compose -f ./docker-compose-test.yml run --rm consumer-test test_v2
# docker-compose -f ./docker-compose-test.yml down
