from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from time import time
import multiprocessing as mp


def process_row(row):
    looping = 0
    hops_complete = 0 
    if len(set(row['hops'])) < row['n_hops']:
        looping = 1
    if len(row['hops']) and row['hops'][-1] == row['dest']:
        hops_complete = 1
    
    row['looping'] = looping
    row['complete'] = hops_complete
    
    return row


def process_data(params):
    thread_id = params[0]
    time_from = params[1]
    time_to = params[2]

    query = {
        "_source":['timestamp','src','dest','traceroute','hops','n_hops','rtts'],
        "query":{
            "bool":{
            "must":[
                {"term":{"src_production":{"value":'true'}}},
                {"term":{"dest_production":{"value":'true'}}},
                {"range":{"timestamp":{
                    "lte":str(time_to),
                    "gte":str(time_from),
                    "format":"epoch_millis"
                }}}
            ]
            }
        }
    }
    
    start_time = time()
    page = es.search(index = 'ps_trace', scroll = '2m', size = 2500, body = query)
    sid = page['_scroll_id']
    scroll_size = page['hits']['total']['value']
    print("Thread {:3d} processing {} docs".format(thread_id, scroll_size))
    i = 0
    while (scroll_size > 0):
        actions = [process_row(result['_source']) for result in page['hits']['hits']]
        bulk_push = 0
        while bulk_push != 1:
            try:
                bulk(es, actions=actions, index='ps_derived_trace', doc_type='doc')
                bulk_push = 1
            except Exception:
                print("Bulk Push Error, Retrying !")
                
        page = es.scroll(scroll_id = sid, scroll = '2m')
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
        if i % 10 == 0:
            total_time = time() - start_time
            mins, secs = divmod(total_time, 60)
            print("Thread Id: {:3d} | Iteration: {:3d} |Time Elapsed: {:4.4f}m {:4.4f}s".format(thread_id, i+1, mins, secs))
        
        i += 1
    
    print("Thread Id: {:3d} Completed | Time Elapsed: {:4.4f}m {:4.4f}s".format(thread_id, mins, secs))


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

    
    # Saved Time Range
    with open("times.txt") as f:
        time_to = float(f.readline().strip())
        time_from = float(f.readline().strip())

    # Window of size 4 days to process by a processor
    window_millis = 4*24*60*60*1000

    # Creating Batches of 4 days each in the time range
    batches = []
    i = 1
    while time_from < time_to:
        batches.append((i, time_from, time_from+window_millis))
        time_from += window_millis
        i += 1
        
    n_threads = len(batches)

    pool = mp.Pool(n_threads)
    results = pool.map(process_data, batches)

    pool.close()
    pool.join()