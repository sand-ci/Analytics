#%% Required Imports
import certifi
from elasticsearch import Elasticsearch

import r_utils as ut

import time
import csv 
import pandas as pd
import numpy as np
from collections import Counter
from multiprocessing import Pool


#%% Reading In Kibana Credentials
with open("creds.key") as f:
    usrname = f.readline().strip()
    passwd = f.readline().strip()

es = Elasticsearch(['atlas-kibana.mwt2.org:9200'], timeout=120, http_auth=(usrname, passwd), scheme= 'ssl')

#%% Flags if we want to get SOURCES and DESTINATIONS
# Otherwise use saved Source and Destinations
GET_SOURCES = 0
GET_DESTS = 0
#%%

def getSourceDestinationPairs(to_date, from_date):
    """
    Get all source and destination pairs
    present in the given time range 
    
    Args:
        to_date:  epoch_millis
        from_date: epoch_millis
    
    Returns:
        Datafame of all source destination pairs
    """
    
    query = {
        "size":0,
        "query":{
            "range":{
                "timestamp":{
                    "gte":from_date,
                    "lte":to_date
                }
            }
        },
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

    data = es.search('ps_trace', body=query)
    
    sources = []
    destinations = []

    for source in data['aggregations']['sources']['buckets']:
        src = source['key']
        for destination in source['destinations']['buckets']:
            sources.append(src)
            destinations.append(destination['key'])
    
    return pd.DataFrame({"Source":sources,
                        "Destinations":destinations})

def getPathCounts(src_ip, dest_ip):
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
    to_date = ut.getDateFormat()
    from_date = ut.getDateFormat(delta=90)

    query = {
        "size":0,
        "query":{
            "bool":{
                "must":[
                    {
                        "range":{
                            "timestamp":{
                                "gte":from_date,
                                "lte":to_date,
                                "format":"epoch_millis"
                            }
                        }
                    },
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
        "aggs":{
            "HashCounts":{
                "terms":{
                    "field":"hash",
                    "size":9999
                }
            }
        }
    }

    
    data = es.search('ps_trace', body=query)
    
    paths = data["aggregations"]["HashCounts"]["buckets"]
    
    if len(paths) == 0:
        return -1 
    else:
        return paths

def topk(src_ip, dest_ip,k=1):
    """
    Returns a  tuple of total paths and paths taken more than k times

    Args:
        src_ip: Source IP, String [ex: "192.168.1.1"]
        dest_ip: Destination IP, String [ex: "192.168.1.5"]


    Returns:
        A tuple of total unique paths and total of paths taken more than k times
        If there are no paths between soure and destination, None is returned
    """
    paths = getPathCounts(src_ip, dest_ip)

    if paths == -1:
        return None

    totalPaths = 0
    moreKPaths = 0

    totalPaths = len(paths)

    for item in paths:
        if item['doc_count'] > k:
            moreKPaths += 1
    
    return (totalPaths, moreKPaths)

#%% Getting All The Source Destination Pairs

src_dest_pairs = getSourceDestinationPairs(ut.getDateFormat(), ut.getDateFormat(delta=90))
src_dest_pairs['Total'] = np.zeros(src_dest_pairs.shape[0])
src_dest_pairs['MoreThanOne'] = np.zeros(src_dest_pairs.shape[0])
src_dest_pairs['Processed'] = np.zeros(src_dest_pairs.shape[0])

#%% Getting Num Paths for Each of the Pairs
def getPaths(min_index, max_index, src_dest_pairs):
    for i in range(min_index, max_index):
        rows = []
        totalTime = 0
        start_time = time.time()
        if src_dest_pairs.iloc[i,4] != 1:
            result = topk(src_dest_pairs.iloc[i,0], src_dest_pairs.iloc[i,1])

            if result is not None:
                src_dest_pairs.iloc[i,2] = result[0]
                src_dest_pairs.iloc[i,3] = result[1]

            src_dest_pairs.iloc[i,4] = 1
        totalTime += time.time() - start_time

#%%
with Pool(8) as p:
    p.map(getPaths, [(0, 100),(101, 150),(150, 200), (200, 250),
    (250,300),(300, 350),(350, 400),(400, 450)])

#%%
#%%90
srcdest.shape
#%%


#%%
