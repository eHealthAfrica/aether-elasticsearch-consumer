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

ES_INSTANCE = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name",
    "url",
    "user",
    "password"
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "The Id Schema",
      "default": "",
      "examples": [
        "the id for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "The Name Schema",
      "default": "",
      "examples": [
        "a nice name for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "url": {
      "$id": "#/properties/url",
      "type": "string",
      "title": "The Url Schema",
      "default": "",
      "examples": [
        "url of the resource"
      ],
      "pattern": "^(.*)$"
    },
    "user": {
      "$id": "#/properties/user",
      "type": "string",
      "title": "The User Schema",
      "default": "",
      "examples": [
        "username for auth"
      ],
      "pattern": "^(.*)$"
    },
    "password": {
      "$id": "#/properties/password",
      "type": "string",
      "title": "The Password Schema",
      "default": "",
      "examples": [
        "password for auth"
      ],
      "pattern": "^(.*)$"
    }
  }
}
'''

KIBANA_INSTANCE = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name",
    "url",
    "user",
    "password"
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "The Id Schema",
      "default": "",
      "examples": [
        "the id for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "The Name Schema",
      "default": "",
      "examples": [
        "a nice name for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "url": {
      "$id": "#/properties/url",
      "type": "string",
      "title": "The Url Schema",
      "default": "",
      "examples": [
        "url of the resource"
      ],
      "pattern": "^(.*)$"
    },
    "user": {
      "$id": "#/properties/user",
      "type": "string",
      "title": "The User Schema",
      "default": "",
      "examples": [
        "username for auth"
      ],
      "pattern": "^(.*)$"
    },
    "password": {
      "$id": "#/properties/password",
      "type": "string",
      "title": "The Password Schema",
      "default": "",
      "examples": [
        "password for auth"
      ],
      "pattern": "^(.*)$"
    }
  }
}
'''

LOCAL_ES_INSTANCE = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name"
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "The Id Schema",
      "default": "",
      "examples": [
        "the id for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "The Name Schema",
      "default": "",
      "examples": [
        "a nice name for this resource"
      ],
      "pattern": "^(.*)$"
    }
  }
}
'''

LOCAL_KIBANA_INSTANCE = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name"
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "The Id Schema",
      "default": "",
      "examples": [
        "the id for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "The Name Schema",
      "default": "",
      "examples": [
        "a nice name for this resource"
      ],
      "pattern": "^(.*)$"
    }
  }
}
'''

SUBSCRIPTION = '''
{
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name",
    "job_id",
    "topic_pattern",
    "es_alias_name",
    "visualizations"
  ],
  "properties": {
    "id": {
      "$id": "#/properties/id",
      "type": "string",
      "title": "The Id Schema",
      "default": "",
      "examples": [
        "the id for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "name": {
      "$id": "#/properties/name",
      "type": "string",
      "title": "The Name Schema",
      "default": "",
      "examples": [
        "a nice name for this resource"
      ],
      "pattern": "^(.*)$"
    },
    "topic_pattern": {
      "$id": "#/properties/topic_pattern",
      "type": "string",
      "title": "The Topic_pattern Schema",
      "default": "",
      "examples": [
        "source topic for data i.e. gather*"
      ],
      "pattern": "^(.*)$"
    },
    "es_alias_name": {
      "$id": "#/properties/es_alias_name",
      "type": "string",
      "title": "The Es_alias_name Schema",
      "default": "",
      "examples": [
        "alias in elasticsearch for this set of topics"
      ],
      "pattern": "^(.*)$"
    },
    "visualizations": {
      "$id": "#/properties/visualizations",
      "type": "array",
      "title": "The Visualizations Schema",
      "items": {
        "$id": "#/properties/visualizations/items",
        "type": "object",
        "title": "The Items Schema",
        "required": [
          "name",
          "title",
          "type",
          "source_field",
          "field_overrides"
        ],
        "properties": {
          "name": {
            "$id": "#/properties/visualizations/items/properties/name",
            "type": "string",
            "title": "The Name Schema",
            "default": "",
            "examples": [
              "name of the visualization"
            ],
            "pattern": "^(.*)$"
          },
          "title": {
            "$id": "#/properties/visualizations/items/properties/title",
            "type": "string",
            "title": "The Title Schema",
            "default": "",
            "examples": [
              "title of the visualization"
            ],
            "pattern": "^(.*)$"
          },
          "type": {
            "$id": "#/properties/visualizations/items/properties/type",
            "type": "string",
            "title": "The Type Schema",
            "default": "",
            "examples": [
              "the kibana visual type to create"
            ],
            "pattern": "^(.*)$"
          },
          "source_field": {
            "$id": "#/properties/visualizations/items/properties/source_field",
            "type": "string",
            "title": "The Source_field Schema",
            "default": "",
            "examples": [
              "which attribute in the source are we plotting"
            ],
            "pattern": "^(.*)$"
          },
          "field_overrides": {
            "$id": "#/properties/visualizations/items/properties/field_overrides",
            "type": "array",
            "title": "The Field_overrides Schema",
            "items": {
              "$id": "#/properties/visualizations/items/properties/field_overrides/items",
              "type": "array",
              "title": "The Items Schema",
              "items": {
                "$id": "#/properties/visualizations/items/properties/field_overrides/items/items",
                "type": "string",
                "title": "The Items Schema",
                "default": "",
                "examples": [
                  "field1_to_set",
                  "value"
                ],
                "pattern": "^(.*)$"
              }
            }
          }
        }
      }
    }
  }
}
'''

ES_JOB = '''
    {
  "definitions": {},
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "oneOf": [{
    "required": [
      "id",
      "name",
      "local_kibana",
      "local_elasticsearch"
    ],
    "properties": {
      "id": {
        "$id": "#/properties/id",
        "type": "string",
        "title": "The Id Schema",
        "default": "",
        "examples": [
          "the id for this resource"
        ],
        "pattern": "^(.*)$"
      },
      "name": {
        "$id": "#/properties/name",
        "type": "string",
        "title": "The Name Schema",
        "default": "",
        "examples": [
          "a nice name for this resource"
        ],
        "pattern": "^(.*)$"
      },
      "local_kibana": {
        "$id": "#/properties/local_kibana",
        "type": "string",
        "title": "The Local_kibana Schema",
        "default": "",
        "examples": [
          "id of the local kibana to use"
        ],
        "pattern": "^(.*)$"
      },
      "local_elasticsearch": {
        "$id": "#/properties/local_elasticsearch",
        "type": "string",
        "title": "The Local_elasticsearch Schema",
        "default": "",
        "examples": [
          "id of the local elasticsearch to use"
        ],
        "pattern": "^(.*)$"
      }
    }
  }, {
    "required": [
      "id",
      "name",
      "kibana",
      "elasticsearch"
    ],
    "properties": {
      "id": {
        "$id": "#/properties/id",
        "type": "string",
        "title": "The Id Schema",
        "default": "",
        "examples": [
          "the id for this resource"
        ],
        "pattern": "^(.*)$"
      },
      "name": {
        "$id": "#/properties/name",
        "type": "string",
        "title": "The Name Schema",
        "default": "",
        "examples": [
          "a nice name for this resource"
        ],
        "pattern": "^(.*)$"
      },
      "kibana": {
        "$id": "#/properties/kibana",
        "type": "string",
        "title": "The Kibana Schema",
        "default": "",
        "examples": [
          "id of the foreign kibana to use"
        ],
        "pattern": "^(.*)$"
      },
      "elasticsearch": {
        "$id": "#/properties/elasticsearch",
        "type": "string",
        "title": "The Elasticsearch Schema",
        "default": "",
        "examples": [
          "id of the foreign elasticsearch to use"
        ],
        "pattern": "^(.*)$"
      }
    }
  }]
}
'''
