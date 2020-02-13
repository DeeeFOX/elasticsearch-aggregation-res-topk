# Elasticsearch Aggregation Result Topk Filter Plugin
=========================================

This plugin adds the possibility to sort and choose top K to return of aggregations result. Results from every bucket will sort by value and then choose top K as the finally result to return. For a complete example see https://github.com/elastic/elasticsearch/issues/xxxxxxx

This is a kind of bucket sort aggregation of pipeline aggregation.

# Installation
------------

* remove

    ```${ES_HOME}/bin/elasticsearch-plugin remove elasticsearch-aggregation-res-topk```

* install

    ```${ES_HOME}/bin/elasticsearch-plugin install file:///path/to/elasticsearch-aggregation-res-topk.zip```

# Build
-----

* change to plugin home

    ```cd ${PLUGIN_HOME}```
    
* run the build

    ```
    gradle clean build
  
    ...
  
    Deprecated Gradle features were used in this build, making it incompatible with Gradle 7.0.
    Use '--warning-mode all' to show the individual deprecation warnings.
    See https://docs.gradle.org/6.1.1/userguide/command_line_interface.html#sec:command_line_warnings
    
    BUILD SUCCESSFUL in 1m 31s
    38 actionable tasks: 35 executed, 3 up-to-date
    ```
  
# Usage
--------------------------

### Parameters
--------------------------

|name|type|required|description|example|
|:--|:--|:--|:--|:--|
|from|int|true|"top k since top n"'s n|1|
|size|int|true|"top k since top n"'s k|2|
|base_key_name|str|true|which name of terms aggs to sort on|ts|
|sort|json|true|order and name of given buckets path to sort|{ "code>ts>res": { "order": "desc" } }|

### Examples
--------------------------

* query

    ```
    {
      "aggs": {
        "topk": {
          "bucket_topk": {
            "from": 1,
            "size": 2,
            "base_key_name": "ts",
            "sort": 
              {
                "code>ts>res": {
                  "order": "desc"
                }
              }
          }
        },
        "code": {
          "terms": {
            "field": "tag.code"
          },
          "aggs": {
            "ts": {
              "date_histogram": {
                "field": "ts",
                "fixed_interval": "20s",
                "min_doc_count": 1
              },
              "aggs": {
                "res": {
                  "sum": {
                    "field": "field.count"
                  }
                }
              }
            }
          }
        }
      },
      "size": 1
    }
    ```
* result

    ```$xslt
    { - 
      "took": 1043,
      "timed_out": false,
      "_shards": { - 
        "total": 2,
        "successful": 2,
        "skipped": 0,
        "failed": 0
      },
      "hits": { - 
        "total": { - 
          "value": 18,
          "relation": "eq"
        },
        "max_score": 1,
        "hits": [ - 
          { - 
            "_index": "test_tsdb12_mama",
            "_type": "_doc",
            "_id": "02B521ed703599128",
            "_score": 1,
            "_routing": "yo_kakaabc_count",
            "_source": { - 
    
            }
          }
        ]
      },
      "aggregations": { - 
        "code": { - 
          "doc_count_error_upper_bound": 0,
          "sum_other_doc_count": 0,
          "buckets": [ - 
            { - 
              "key": "400",
              "doc_count": 4,
              "ts": { - 
                "buckets": [ - 
                  { - 
                    "key_as_string": "2020-01-01T00:59:20.000Z",
                    "key": 1577840360000,
                    "doc_count": 1,
                    "res": { - 
                      "value": 16
                    }
                  },
                  { - 
                    "key_as_string": "2020-01-01T00:59:40.000Z",
                    "key": 1577840380000,
                    "doc_count": 1,
                    "res": { - 
                      "value": 16
                    }
                  },
                  { - 
                    "key_as_string": "2020-01-01T01:59:00.000Z",
                    "key": 1577843940000,
                    "doc_count": 1,
                    "res": { - 
                      "value": 16
                    }
                  },
                  { - 
                    "key_as_string": "2020-01-01T01:59:40.000Z",
                    "key": 1577843980000,
                    "doc_count": 1,
                    "res": { - 
                      "value": 16
                    }
                  }
                ]
              }
            },
            { - 
              "key": "200",
              "doc_count": 3,
              "ts": { - 
                "buckets": [ - 
                  { - 
                    "key_as_string": "2020-01-01T00:59:00.000Z",
                    "key": 1577840340000,
                    "doc_count": 1,
                    "res": { - 
                      "value": 16
                    }
                  },
                  { - 
                    "key_as_string": "2020-01-01T00:59:20.000Z",
                    "key": 1577840360000,
                    "doc_count": 1,
                    "res": { - 
                      "value": 16
                    }
                  },
                  { - 
                    "key_as_string": "2020-01-01T01:59:40.000Z",
                    "key": 1577843980000,
                    "doc_count": 1,
                    "res": { - 
                      "value": 16
                    }
                  }
                ]
              }
            },
            { - 
              "key": "300",
              "doc_count": 2,
              "ts": { - 
                "buckets": [ - 
                  { - 
                    "key_as_string": "2020-01-01T01:59:20.000Z",
                    "key": 1577843960000,
                    "doc_count": 2,
                    "res": { - 
                      "value": 32
                    }
                  }
                ]
              }
            }
          ]
        },
        "topk": { - 
          "buckets": [ - 
            { - 
              "key": { - 
                "code": "200",
                "ts": "2020-01-01T00:59:20.000Z"
              },
              "doc_count": 1,
              "res": { - 
                "value": 16
              }
            },
            { - 
              "key": { - 
                "code": "200",
                "ts": "2020-01-01T01:59:40.000Z"
              },
              "doc_count": 1,
              "res": { - 
                "value": 16
              }
            }
          ]
        }
      }
    }
    ```
# Version
--------------------------

Plugin versions are available for (at least) all minor versions of Elasticsearch since 7.4.2

The first 3 digits of plugin version is Elasticsearch versioning. The last digit is used for plugin versioning under an elasticsearch version.

To install it, launch this command in Elasticsearch directory replacing the url by the correct link for your Elasticsearch version (see table)

| elasticsearch version | plugin version(stable) | plugin url(source code) |
| --------------------- | -------------- | ---------- |
| 7.4.2 | 7.4.2.0 | https://github.com/DeeeFOX/elasticsearch-aggregation-res-topk |

