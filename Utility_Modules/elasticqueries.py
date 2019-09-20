import Utility_Modules.r_utils as ut

def getUniqueCount(es, index, field, time_from, time_to):
    '''
    Get Unique Count returns the distinct count of the field in the index.
    
    es: ElasticSearch Connection Object
    Field : Attribute In the Index (String)
    Index: Index in which we want ot search the field.
    '''
    
    query = {
    "size":0,
    "query":{
      "bool":{
        "must":[
          {
            "range":{
              "timestamp":{
              "gte":time_from,
              "lte":time_to,
              "format": "epoch_millis"
              }
            }
          },
          {
            "term":{
              "src_production":{
                "value":"true"
              }
            }
          },
          {
            "term":{
              "dest_production":{
                "value":"true"
              }
            }
          }
        ]
      }
    },
    'aggs':{
        'uniq_val':{
            'cardinality':{
                'field':field,
                }
            }
        }    
    }

    try:
        result = es.search(index='ps_trace', body=query)
        val = result['aggregations']['uniq_val']['value']
        return val
    except Exception as e:
        print(e)
        return None

    
def getUniqueCountBy(es, index, field, time_from, time_to):
    '''
    Get Unique Count returns the distinct count of the for each value of field in the index.
    Field : Attribute In the Index (String)
    Index: Index in which we want ot search the field.
    
    Prints the number of buckets that will be returned.
    '''
    sz = getUniqueCount(es,index, field, time_from, time_to)
    print("Size : {}".format(sz))
    
    query = {
        "size":0,
        "query":{
          "bool":{
            "must":[
              {
                "range":{
                  "timestamp":{
                    "gte":time_from,
                    "lte": time_to,
                    "format": "epoch_millis"
                  }
                }
              },
              {
                "term":{
                  'src_production':{
                    "value":"true"
                  }
                }
              },
              {
                "term":{
                  "dest_production":{
                    "value":"true"
                  }
                }
              }
            ]
          }
        },
        "aggs":{
            "FieldCounts":{
                "terms":{
                    "field":field,
                    "size":sz
                }
            }
        }
    }
    
    try:
        result = es.search(index=index, body=query)
        val = result['aggregations']['FieldCounts']['buckets']
        return val
    except Exception as e:
        print(e)
        return None
    
    
def getNumHashesBetweenHostsInTimeRange(es, index, time_from, time_to):
    '''
    es: Elastic Search connection object
    index: Index to be searched/scanned within
    Time Range
    Time Format: epoch_milliseconds
    '''
    
    pre_query = {
    "query": {
      "bool":{
        "must":[
          {
            "range":{
              "timestamp":{
              "gte":time_from,
              "lte":time_to,
              "format": "epoch_millis"
              }
            }
          },
          {
            "term":{
              "src_production":{
                "value":"true"
              }
            }
          },
          {
            "term":{
              "dest_production":{
                "value":"true"
              }
            }
          }
        ]
      }
    }, 
    "size":0,
    "aggs":{
        
        "uniq_val":{
            "cardinality":{
                    "script":{
                      "source": "doc['src_host'].value + ',' + doc['dest_host'].value",
                      "lang": "painless"
                    }
                }
            }
        }    
    }

    pre_result = es.search(index, body = pre_query)
    sz = pre_result['aggregations']['uniq_val']['value']
    print("Number of Source-Destination Pairs: ",sz)
    
    query = {
    "size": 0, 
    "query": {
      "bool":{
        "must":[
          {
            "range": {
              "timestamp": {
              "gte": time_from,
              "lte": time_to,
              "format":"epoch_millis"
              }
            }
          },
          {
            "term":{
              'src_production':{
                "value":"true"
              }
            }
          },
          {
            "term":{
              "dest_production":{
                "value":"true"
              }
            }
          }
        ]
      }
    },
    "aggs":{
        "uniq_val":{
            "terms":{
                    "script":{
                      "source": "doc['src_host'].value + ',' + doc['dest_host'].value",
                      "lang": "painless"
                    },
                "size":sz,
                },
            "aggs":{
                "uniq_hash":{
                    "cardinality":{
                        "field":"hash"
                    }
                }
            }
          }
        }    
    }
    
    X = es.search(index, body=query, request_timeout=60)
    
    return X 


def getDailyUniquePaths(es, index, src, dest, since):
    """
    Get number of unique paths from 
    src : Source (String)
    dest: Destination (String) 
    since: how many past days
    """
    toDate = ut.getDateFormat(delta = 1)
    fromDate = ut.getDateFormat(delta = since)
    
    query = {
      "size": 0,
      "query": {
        "bool": {
          "must": [
            {
              "range": {
                "timestamp": {
                  "gte": fromDate,
                  "lte": toDate,
                  "format": "epoch_millis"
                }
              }
            },
            {
              "term": {
                "src_host": {
                  "value": src
                }
              }
            },
            {
              "term": {
                "dest_host": {
                  "value": dest
                }
              }
            },
            {
              "term":{
                "src_production":"true"
              }
            },
            {
              "term":{
                "dest_production":"true"
              }
            }
          ]
        }
      },
      "aggs": {
        "time_hist": {
          "date_histogram": {
            "field": "timestamp",
            "interval": "day"
          },
          "aggs": {
            "uniq_hash": {
              "cardinality": {
                "field": "hash"
              }
            }
          }
        }
      }
    }
    
    return es.search(index, body=query)  


def getSourceDestinationPairs(es, index):
    """
    Get all source and destination pairs
    present in the given time range 
    
    Args:
        es: Elasticsearch 
        index: INdex in Elastic Search
    
    Returns:
        Datafame of all source destination pairs
    """
    
    query = {
        "size":0,
        "aggs":{
            "sources":{
                "terms":{
                    "field":"src",
                    "size":9999
                },
                "aggs":{
                    "destinations":{
                        "terms":{
                            "field":"dest",
                            "size":9999
                        }
                    }
                }
            }
        }
    }

    data = es.search(index, body=query)
    
    sources = []
    destinations = []

    for source in data['aggregations']['sources']['buckets']:
        src = source['key']
        for destination in source['destinations']['buckets']:
            sources.append(src)
            destinations.append(destination['key'])
    
    return {"Source":sources, "Destinations":destinations}


def getPathCounts(es, src_ip, dest_ip):
    """
    Returns a list of Counts of Paths taken from given source and destination

    Args:
        src_ip: Source IP, String [ex: "192.168.1.1"]
        dest_ip: Destination IP, String [ex: "192.168.1.5"]
    
    Returns:
        A list of dictionaries. The dictionary looks as follows:
        {
            'key':HASH VALUE,
            'doc_count': # of times path taken
        }
    """

    query = {
        "size":0,
        "query":{
            "bool":{
                "must":[
                    {
                        "term":{
                            "src":{
                                "value":src_ip
                            }
                        }
                    },
                    {
                        "term":{
                            "dest":{
                                "value":dest_ip
                            }
                        }
                    },
                ]
            }
        },
        "aggs":{
            "HashCounts":{
                "terms":{
                    "field":"hash",
                    "size":9999
                }
            }
        }
    }

    data_flag = 0
    while data_flag == 0:
        try:
            data = es.search('ps_derived_complete_traces', body=query)
            data_flag = 1
        except Exception:
            print("ERROR in getPathCounts", src_ip, dest_ip, "\n")
    
    paths = data["aggregations"]["HashCounts"]["buckets"]
    
    if len(paths) == 0:
        return -1 
    else:
        return paths


def getPathReadTime(es, path, time_to, time_from, size):
    """
    Gets the timestamps for the hash provided in the given time range
    
    Args:
        es: Elastic Search Object
        path: Hashed value of the path
        time_to: Time range start in epoch millisecond
        time_from: Time range end in epoch millisecond
        size: Number of readings for the path
    Returns:
        A list of time-stamps (epoch_millis) on which the path was recorded 
    """
    
    query = {
        "_source":['timestamp'],
        "size":size,
        "query":{
            "bool":{
                "must":[
                    {
                        "range":{
                            "timestamp":{
                                "gte":time_from,
                                "lte":time_to,
                                "format":"epoch_millis"
                            }
                        }
                    },
                    {
                        "term":{
                            "src_production":{
                                "value":"true"
                            }
                        }
                    },
                    {
                        "term":{
                            "dest_production":{
                                "value":"true"
                            }
                        }
                    },
                    {
                        "term":{
                            "hash":{
                                "value":path
                            }
                        }
                    }
                ]
            }
        }
    }
    
    data_flag = 0
    while data_flag == 0:
        try:
            data = es.search('ps_trace', body=query,filter_path=['hits.hits._source.timestamp'])
            data_flag = 1
        except Exception:
            print("Error in PairPaths | src:{} | dest:{}".format(src, dest))

    results = []
    
    for hit in data['hits']['hits']:
        results.append(hit['_source']['timestamp'])
    
    return results
