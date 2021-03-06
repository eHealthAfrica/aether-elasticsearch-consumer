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
    "url"
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
  "definitions": { },
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name",
    "url"
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
  "definitions": { },
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
  "definitions": { },
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
  "definitions": { },
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "required": [
    "id",
    "name",
    "topic_pattern"
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
    "topic_options": {
      "$id": "#/properties/topic_options",
      "type": "object",
      "title": "The Topic_options Schema",
      "anyOf": [
        {"required": ["masking_annotation"]},
        {"required": ["filter_required"]}
      ],
      "dependencies":{
        "filter_required": ["filter_field_path", "filter_pass_values"],
        "masking_annotation": ["masking_levels", "masking_emit_level"]
      },
      "properties": {
        "masking_annotation": {
          "$id": "#/properties/topic_options/properties/masking_annotation",
          "type": "string",
          "title": "The Masking_annotation Schema",
          "default": "",
          "examples": [
            "@aether_masking"
          ],
          "pattern": "^(.*)$"
        },
        "masking_levels": {
          "$id": "#/properties/topic_options/properties/masking_levels",
          "type": "array",
          "title": "The Masking_levels Schema",
          "items": {
            "$id": "#/properties/topic_options/properties/masking_levels/items",
            "title": "The Items Schema",
            "examples": [
              "private",
              "public"
            ],
            "pattern": "^(.*)$"
          }
        },
        "masking_emit_level": {
          "$id": "#/properties/topic_options/properties/masking_emit_level",
          "type": "string",
          "title": "The Masking_emit_level Schema",
          "default": "",
          "examples": [
            "public"
          ],
          "pattern": "^(.*)$"
        },
        "filter_required": {
          "$id": "#/properties/topic_options/properties/filter_required",
          "type": "boolean",
          "title": "The Filter_required Schema",
          "default": false,
          "examples": [
            false
          ]
        },
        "filter_field_path": {
          "$id": "#/properties/topic_options/properties/filter_field_path",
          "type": "string",
          "title": "The Filter_field_path Schema",
          "default": "",
          "examples": [
            "some.json.path"
          ],
          "pattern": "^(.*)$"
        },
        "filter_pass_values": {
          "$id": "#/properties/topic_options/properties/filter_pass_values",
          "type": "array",
          "title": "The Filter_pass_values Schema",
          "items": {
            "$id": "#/properties/topic_options/properties/filter_pass_values/items",
            "title": "The Items Schema",
            "examples": [
              false
            ]
          }
        }
      }
    },
    "es_options": {
      "$id": "#/properties/es_options",
      "type": "object",
      "title": "The Es_options Schema",
      "required": [
      ],
      "properties": {
        "alias_name": {
          "$id": "#/properties/es_options/properties/alias_name",
          "type": "string",
          "title": "The Alias_name Schema",
          "default": "",
          "examples": [
            "test"
          ],
          "pattern": "^(.*)$"
        },
        "auto_timestamp": {
          "$id": "#/properties/es_options/properties/auto_timestamp",
          "type": ["null", "string", "boolean"],
          "title": "The Auto_timestamp Schema",
          "examples": [
            "my-timestamp"
          ]
        },
        "geo_point_creation": {
          "$id": "#/properties/es_options/properties/geo_point_creation",
          "type": "boolean",
          "title": "The Geo_point_creation Schema",
          "default": false,
          "examples": [
            true
          ]
        },
        "geo_point_name": {
          "$id": "#/properties/es_options/properties/geo_point_name",
          "type": "string",
          "title": "The Geo_point_name Schema",
          "default": "",
          "examples": [
            "geopoint"
          ],
          "pattern": "^(.*)$"
        }
      }
    },
    "kibana_options": {
      "$id": "#/properties/kibana_options",
      "type": "object",
      "title": "The Kibana_options Schema",
      "required": [
      ],
      "properties": {
        "auto_visualization": {
          "$id": "#/properties/kibana_options/properties/auto_visualization",
          "type": "string",
          "title": "The Auto_visualization Schema",
          "enum": ["full", "schema", "none"],
          "examples": [
            "full"
          ],
          "pattern": "^(.*)$"
        }
      }
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
  "definitions": { },
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$id": "http://example.com/root.json",
  "type": "object",
  "title": "The Root Schema",
  "oneOf": [
    {
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
        },
        "subscription": {
          "$id": "#/properties/subscription",
          "type": "array",
          "title": "The Subscriptions Schema",
          "items": {
            "$id": "#/properties/subscription/items",
            "type": "string",
            "title": "The Items Schema",
            "default": "",
            "examples": [
              "id-of-sub"
            ],
            "pattern": "^(.*)$"
          }
        }
      }
    },
    {
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
        },
        "subscriptions": {
          "$id": "#/properties/subscriptions",
          "type": "array",
          "title": "The Subscriptions Schema",
          "items": {
            "$id": "#/properties/subscriptions/items",
            "type": "string",
            "title": "The Items Schema",
            "default": "",
            "examples": [
              "id-of-sub"
            ],
            "pattern": "^(.*)$"
          }
        }
      }
    }
  ]
}
'''
