#%%%
from itertools import chain

from elasticsearch import Elasticsearch
from neo4j import GraphDatabase

import pandas as pd
import numpy as np

import r_utils as ut


#%%
user = 'sushant'
passwd = 'Mross@fmaB'
credentials = (user, passwd)
es = Elasticsearch(['atlas-kibana.mwt2.org:9200'], timeout=90, http_auth=credentials)
es.ping()


#%%

src_ip  = "193.239.180.213"
dest_ip = "109.105.125.233"

to_date = ut.getDateFormat()
from_date = ut.getDateFormat(delta=181)

query = {
    "size":5000,
    "_source":["hops"],
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
                }
            ]
        }
    }
}


#%%
X = es.search('ps_trace', body=query)

#%% [markdown]
#### Getting Unique Paths from the paths given

#%%

pathDict = {}

for result in X['hits']['hits']:
    temp = ",".join(result['_source']['hops'])
    try:
        pathDict[temp] += 1
    except:
        pathDict[temp] = 1
#%%
pathDict

#%%
paths = []
for key in pathDict.keys():
    if pathDict[key] >= 5:
        paths.append(["SOURCE"]+key.split(",")+["DESTINATION"])

nodes = chain.from_iterable(paths)
nodes = set([i for i in nodes if len(i) > 0])
nodes



#%%[markdown]
# #### Now sending data to Neo4j
# `i.e. creating path graph`

#%%

uri = 'bolt://localhost:7687'
user = 'neo4j'
pawd = '1234'
neo = GraphDatabase.driver(uri, auth=(user, pawd))

# nodes = [i+1 for i in range(10)]
# nodes

#%%
paths

#%%
with neo.session() as session:
    for node in nodes:
        session.run("CREATE (n:Node{{IP:'{}'}})".format(str(node)))

#%%

with neo.session() as session:
    # session.run("CREATE INDEX ON :Node(IP)")
    for path in paths:
        for i in range(len(path)-1):
            session.run("MATCH (s:Node{{IP:'{}'}}), (d:Node{{IP:'{}'}}) MERGE (s)-[:TO]->(d)".format(path[i],path[i+1]))



#%%



