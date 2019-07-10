#%%%
from elasticsearch import Elasticsearch
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
from_date = ut.getDateFormat(delta=91)

query = {
    "size":5,
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

#%%

pathDict = {}

for result in X['hits']['hits']:
    temp = ",".join(result['_source']['hops'])
    try:
        pathDict[temp] += 1
    except:
        pathDict[temp] = 1
#%%