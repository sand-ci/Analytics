import Utility_Modules.r_utils as ut

def getUniqueCount(es, index, field):
    '''
    Get Unique Count returns the distinct count of the field in the index.
    
    es: ElasticSearch Connection Object
    Field : Attribute In the Index (String)
    Index: Index in which we want ot search the field.
    '''
    
    query = {
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

    
def getUniqueCountBy(es, index, field):
    '''
    Get Unique Count returns the distinct count of the for each value of field in the index.
    Field : Attribute In the Index (String)
    Index: Index in which we want ot search the field.
    
    Prints the number of buckets that will be returned.
    '''
    sz = getUniqueCount(es,index, field)
    print("Size : {}".format(sz))
    
    query = {
        "size":0,
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
      "range": {
        "timestamp": {
          "gte": time_from,
          "lte": time_to,
          "format":"epoch_millis"
        }
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
      "range": {
        "timestamp": {
          "gte": time_from,
          "lte": time_to
        }
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

'''
def getDailyAveragePaths(es, index, since):
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
            "terms":{
                    "script":{
                      "source": "doc['src_host'].value + ',' + doc['dest_host'].value",
                      "lang": "painless"
                    },
                "size":sz,
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
              "avg": {
                "field": "hash"
              }
            }
          }
        }
      }
    }
    
    return es.search(index, body=query)  
'''