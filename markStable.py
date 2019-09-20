from elasticsearch import Elasticsearch
from time import time, strftime, localtime
from datetime import datetime, timedelta
from itertools import islice
import numpy as np
import pandas as pd
import json
import multiprocessing as mp

import Utility_Modules.r_utils as ut
import Utility_Modules.elasticqueries as qrs

def consumeIter(iterator, n = None):
    "Advance the iterator n-steps ahead. If n is None, consume entirely."
    # Use functions that consume iterators at C speed.
    if n is None:
        # feed the entire iterator into a zero-length deque
        collections.deque(iterator, maxlen=0)
    else:
        # advance to the empty slice starting at position n
        next(islice(iterator, n, n), None)

        
def markStablePairPaths(es, src, dest, path_dict, threshold):
    """
    Marks a single pair paths stable/unstable
    
    Args:
        es: ElasticSearch object
        src: Source IP
        dest: dest IP
        path_dict: Dictionary of the paths between the pair. [Results will be stored in this dict only]
        threshold: Amount of Consequetive reading to consider a path stable
    
    Returns:
        None
    
    """
    query = {
        "_source":['hash', 'timestamp'],
        "size":9999,
        "query":{
            "bool":{
                "must":[
                    {"term":{"complete":{"value":1}}},
                    {"term":{"src":{"value":src}}},
                    {"term":{"dest":{"value":dest}}}
                ]
            }
        }
    }
    
    
    times = []
    paths = []

    is_page = 0
    while is_page == 0:
        try:
            page = es.search(index = 'ps_derived_complete_traces', body = query, scroll='2m', size=1000)
            is_page = 1
        except:
            print("Error in retreiving timestamp data for the pair, retrying !:")
            sleep(0.1)
    
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']['value']
    
    while scroll_size > 0:
        for res in page['hits']['hits']:
            times.append(res['_source']['timestamp'])
            paths.append(res['_source']['hash'])
        is_page = 0
        while is_page == 0:
            try:
                page = es.scroll(scroll_id = sid, scroll='2m')
                is_page = 1 
            except:
                print("Error in retreiving timestamp data for the pair, retrying !:")
                sleep(0.1)
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
                
    data_frame = pd.DataFrame({"Time":times, "Path":paths}).sort_values(by=['Time'])
    data_iterator = iter(range(data_frame.shape[0]-threshold))
    for indx in data_iterator:
        flag = 0
        if path_dict.get(data_frame.iloc[indx,1]) != 1:
            for i in range(indx,indx+threshold-1):
                if data_frame.iloc[i,1] != data_frame.iloc[i+1,1]:
                    flag = 1
                    break
            if flag == 0:
                path_dict[data_frame.iloc[indx,1]] = 1
                consumeIter(data_iterator, threshold)

def markStable(args):   
    """
    Marks paths between pairs as stable or unstable
    
    Args:
        pairs : Pandas df contaning columns containing src and dest
        threshold : Amount of readings per hour
    
    Returns:
        List of Dictionaries of type:
        {
            "source":<SRC>,
            "destination":<DEST>,
            "paths":{P1:1, P2:0 .... Pn:1}
        }
    """
    
    pair, threshold, thread_id = args[0], args[1], args[2]
    print("Thread : {} , Processing: {} Pairs".format(thread_id, pair.shape[0]))
    paths_stability = []
    
    start_time = time()
    for indx in range(pair.shape[0]):
        temp_res = {
            "source":pair.iloc[indx,0],
            "destination":pair.iloc[indx,1],
            "path_dict":{}
        }
        p_dict_t = qrs.getPathCounts(es, pair.iloc[indx,0], pair.iloc[indx,1])
        p_dict= {path['key']:0 for path in p_dict_t}
        
        markStablePairPaths(es, pair.iloc[indx,0], pair.iloc[indx,1], path_dict=p_dict, threshold=threshold)
        
        temp_res['path_dict'] = p_dict
        paths_stability.append(temp_res)
        
        if indx % 25 == 0:
            mins, secs = divmod(time()-start_time, 60)
            print("Thread : {} | Processed : {} pairs | Elapsed: {}m {}s".format(thread_id, indx, mins, secs))
        
    return paths_stability


if __name__ == "__main__":

    user = None
    passwd = None
    
    if user is None and passwd is None:
        with open("creds.key") as f:
            user = f.readline().strip()
            passwd = f.readline().strip()

    credentials = (user, passwd)
    es = Elasticsearch(['atlas-kibana.mwt2.org:9200'], timeout = 180, http_auth=credentials)

    if es.ping() == True:
        print("Connection Successful") 
    else: 
        print("Connection Unsuccessful")

    #Getting the Pairs:

    pairs = qrs.getSourceDestinationPairs(es, 'ps_derived_complete_traces')
    pairs = pd.DataFrame(pairs)

    print("pairs Retreived")

    THRESHOLD = 5
    n_threads = 16
    pair_pieces = np.array_split(pairs, n_threads)

    pool = mp.Pool(n_threads)
    results = pool.map(markStable, [[pair_pieces[i], THRESHOLD, i+1] for i in range(n_threads)])

    pool.join()
    pool.close()

    result = []

    for i in results:
        result += i

    with open("Results.json") as f:
        f.write(json.dumps({"PathStability":result}))

