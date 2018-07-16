## Aether Elasticsearch Kafka Consumer

This is an Aether Elasticsearch Kafka Consumer, which consumes datа from Kafka and feeds
into an Elasticsearch instance.

### Running the app

To run the app just type:

```
docker-compose up
```

### Configuration

#### `conf/consumer/kafka.json`

Connection to a Kafka instance can be configured via `conf/consumer/kafka.json`. This is a sample of its shape
and data:

```json
{
    "bootstrap_servers" : ["kafka:29092"], 
    "auto_offset_reset" : "earliest",
    "aether_emit_flag_required" : false,
    "aether_masking_schema_levels" : ["false", "true"],
    "aether_masking_schema_emit_level": "false",
    "heartbeat_interval_ms": 2500,
    "session_timeout_ms": 18000,
    "request_timeout_ms": 20000,
    "consumer_timeout_ms": 17000
}
```

### Running the tests

To run the tests type the following command which also checks for PEP8 errors:

```
docker-compose -f docker-compose.test.yml up --build
```

