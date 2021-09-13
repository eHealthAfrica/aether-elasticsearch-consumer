# Aether Elasticsearch Kafka Consumer

This is an Aether Elasticsearch Kafka Consumer, which consumes datа from Kafka and feeds
into Elasticsearch instances.

The consumer runs on the Aether Consumer SDK, and can function in the multi-tenanted Aether environment.

## Running the app

The simplest way to try the consumer, is to use `aether-bootstrap` and opt for Elasticsearch integration. The after system setup, consumer API will then be available at `/{tenant}/es-consumer`. Please refer to the `aether-bootstrap` documentation for more details on getting started.

## Configuration

### `conf/consumer/kafka.json`

To setup the main consumer, the `kafka.json` file should match your preferred Kafka settings. This is not user facing. The consumer running multi-tenanted assumes that you have employed topic level access control, and that a user coming from `{tenant}` should be allowed to read topics matching `^{tenant}.*`.

You can also set the default masking and message filtering settings here, but if specified, the user's rules will take precedence.

```json
{
    "aether_emit_flag_required" : false,
    "aether_masking_schema_emit_level": "false",
    "aether_masking_schema_levels" : ["false", "true"],
    "auto_offset_reset" : "earliest",
    "consumer_timeout_ms": 17000,
    "heartbeat_interval_ms": 2500,
    "request_timeout_ms": 20000,
    "session_timeout_ms": 18000
}
```

## Usage

As with all consumers built on the SDK, tasks are driven by a Job which has a set of Resources. In this case, a Job has a `subscription` to a topic (or wildcard) on Kafka, and sends data to a pair of `elasticsearch` and `kibana` instance. All resource examples and schemas can be found in `/consumer/app/fixtures`.

### Elasticsearch

(post to `/elasticsearch/add` as json)

```json
{
    "id": "es-test",
    "name": "Test ES Instance",
    "url": "http://elasticsearch:9200",
    "user": "admin",
    "password": "admin"
}
```

### Kibana

(post to `/kibana/add` as json)

```json
{
    "id": "k-test",
    "name": "Test Kibana Instance",
    "url": "http://kibana:5601/kibana-app",
    "user": "admin",
    "password": "admin"
}
```

### Subscription

(post to `/subscription/add` as json)

```json
{
    "id": "sub-test",
    "name": "Test Subscription",
    "topic_pattern": "*",
    "topic_options": {
        "masking_annotation": "@aether_masking",
        "masking_levels": ["public", "private"],
        "masking_emit_level": "public",
        "filter_required": true,
        "filter_field_path": "operational_status",
        "filter_pass_values": ["operational"]
    },
    "es_options": {
        "alias_name": "test",
        "auto_timestamp": true,
        "geo_point_creation": true,
        "geo_point_name": "geopoint"
    },
    "kibana_options": {
        "auto_visualization": "schema"
    },
    "visualizations": []
}
```

### Job

Finally we, tie it together with a Job that references the above artifacts by ID.
(post to `/job/add` as json)

```json
{
    "id": "j-test-foreign",
    "name": "ES Consumer Job",
    "kibana": "k-test",
    "elasticsearch": "es-test",
    "subscription": ["sub-test"]
}
```

If the consumer is configured to use a default, multi-tenanted Elasticsearch/Kibana cluster, you can create artifacts via the `local_elasticsearch` && `local_kibana` API endpoints, and then reference them in a job like so:

```json
{
    "id": "j-test-local",
    "name": "ES Consumer Job using Local Cluster",
    "local_kibana": "default",
    "local_elasticsearch": "default",
    "subscription": ["sub-test"]
}
```

## Control and Artifact Functions

The Aether Consumer SDK allows exposure of functionality on a per Job or per Resource basis. You can query for a list of available functions on any of the artifacts by hitting its `describe` endpoint.
For example; `/job/describe?id=j-test-foreign` yields:

```json
[
  {
    "doc": "Described the available methods exposed by this resource type",
    "method": "describe",
    "signature": "(*args, **kwargs) -> List[aet.resource.MethodDesc]"
  },
  {
    "doc": "Returns the schema for instances of this resource",
    "method": "get_schema",
    "signature": "(*args, **kwargs)"
  },
  {
    "doc": "Return a lengthy validations. {'valid': True} on success {'valid': False, 'validation_errors': [errors...]} on failure",
    "method": "validate_pretty",
    "signature": "(definition, *args, **kwargs)"
  },
  {
    "doc": "[GET / POST]  Retrieve an instance of this type.  Requires argument {id}",
    "method": "get",
    "signature": "id:str"
  },
  {
    "doc": "[POST (json)]  Create a new instance of this type.  Requires the Resource Definition for the new instance as json body",
    "method": "add",
    "signature": "json_body: ResourceDefinition"
  },
  {
    "doc": "[GET / POST]  Delete an instance of this type.  Requires argument {id}",
    "method": "delete",
    "signature": "id:str"
  },
  {
    "doc": "[GET]  List existing instance of this type",
    "method": "list",
    "signature": "()"
  },
  {
    "doc": "[POST]  Validate a ResourceDefinition against the type schema.  Requires the Resource Definition for the new instance as json body",
    "method": "validate",
    "signature": "json_body: ResourceDefinition"
  },
  {
    "doc": "Temporarily Pause a job execution.  Will restart if the system resets. For a longer pause, remove the job via DELETE",
    "method": "pause",
    "signature": "(self, *args, **kwargs)"
  },
  {
    "doc": "Resume the job after pausing it.",
    "method": "resume",
    "signature": "(self, *args, **kwargs)"
  },
  {
    "doc": null,
    "method": "get_status",
    "signature": "(self, *args, **kwargs) -> Union[Dict[str, Any], str]"
  },
  {
    "doc": "A list of the last 100 log entries from this job in format:  [(timestamp, log_level, message), (timestamp, log_level, message),...  ]",
    "method": "get_logs",
    "signature": "(self, *arg, **kwargs)"
  },
  {
    "doc": "Get a list of topics to which the job can subscribe.  You can also use a wildcard at the end of names like: Name* which would capture both Name1 && Name2, etc",
    "method": "list_topics",
    "signature": "(self, *args, **kwargs)"
  },
  {
    "doc": "A List of topics currently subscribed to by this job",
    "method": "list_subscribed_topics",
    "signature": "(self, *arg, **kwargs)"
  },
  {
    "doc": "A List of topics currently assigned to this consumer",
    "method": "list_assigned_topics",
    "signature": "(self, *arg, **kwargs)"
  }
]
```
