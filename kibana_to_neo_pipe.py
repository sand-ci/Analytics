from itertools import chain
import certifi

from elasticsearch import Elasticsearch
from neo4j import GraphDatabase

import pandas as pd
import numpy as np

import r_utils as ut


class KibanaExtractor:
    """
    This class helps move trace data from Kibana (Elastic Search) to Neo4j.
    """
    
    def __init__(self, host_addresss,usrname, passwd, timeout = 90):
        self.es = Elasticsearch([host_addresss], http_auth = (usrname, passwd) ,timeout = 90, scheme = 'ssl')

    def getdata(self, src_ip, dest_ip, since = 21, toDate=None, fromDate=None):
        pass
    
    def getuniquenodes(self, data=None):
        pass

    def get_paths(self, data = None, topk = None, relations = False):
        pass


class NeoInjector:
    '''
    '''

    def __init__(
        self, 
        host_address, 
        usrname, 
        passwd):
        self.neo = GraphDatabase.driver(uri=host_address, auth=(usrname, passwd))
    
    def send_nodes_to_neo(
        self, 
        values, 
        attribute_name = 'IP'):
        
        with self.neo.session() as session:
            for val in values:
                session.run("CREATE (n:Node{{{}:'{}'}})".format(attribute_name ,str(val)))

    def send_relations_to_neo(
        self, 
        data, 
        attribute_name = 'IP', 
        relation_name = 'TO'):
    
        with self.neo.session() as session:
            for relation in data:
                session.run("MATCH (s:Node{{{}:'{}'}}), (d:Node{{{}:'{}'}}) MERGE (s)-[:TO]->(d)".format(attribute_name, relation[0], attribute_name, relation[1]))