from itertools import chain
import certifi
import sys

from elasticsearch import Elasticsearch
import neo4j
from neo4j import GraphDatabase

import pandas as pd
import numpy as np

import Utility_Modules.r_utils as ut

class KibanaExtractor:
    """
    This class helps get data from Kibana (Elastic Search) to be sent Neo4j.
    """
    
    def __init__(self, host_addresss,usrname, passwd, timeout = 90):
        """
        Initialize the Connection to Neo4j

        Args:
            host_address: Address for the ElasticSearch Instance
            usrname: Username for Authentication
            passwd : Password for Authentication
            timeout: Connection Timeout in Seconds, Default: 90s
        Returns:
            None
        
        Raises:
            None
        """
        self.es = Elasticsearch([host_addresss], http_auth = (usrname, passwd) ,timeout = 90, scheme = 'ssl')
        self.data = None
        self.nodes = None
        self.paths = None

    def getdata(self, src_ip, dest_ip, since = 90, toDate=None, fromDate=None):
        """
        """
        self.src = src_ip
        self.dest = dest_ip
        if toDate is None and fromDate is None:
            toDate = ut.getDateFormat()
            fromDate = ut.getDateFormat(delta=since)
        elif toDate is not None or fromDate is not None:
            print("Please provide both to and from date in epoch miliseconds. Or don't provide either")

        query = {
            "size":9999,
            "_source":["hops"],
            "query":{
                "bool":{
                    "must":[
                        {
                            "range":{
                                "timestamp":{
                                    "gte":fromDate,
                                    "lte":toDate,
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

        data = self.es.search('ps_trace', body=query)['hits']['hits']

        path_dict = {}

        for result in data:
            tmp = ",".join(result['_source']['hops'])
            try:
                path_dict[tmp] += 1
            except:
                path_dict[tmp] = 1
        
        self.data = path_dict

    def getuniquenodes(self):
        if self.nodes is not None:
            return self.nodes
        else:
            if self.get_paths() is None:
                return
            self.nodes = set([i for i in chain.from_iterable(self.paths) if len(i)>0])

            return self.nodes

    def get_paths(self, k = 0):
        if self.data is None:
            print("Please call getData() before using this function")
            return None
        paths = []
        for key in self.data.keys():
            if self.data[key] > k:
                paths.append(["SOURCE : "+self.src]+key.split(",")+["DESTINATION : "+self.dest])
        self.paths = paths
        return paths

class NeoInjector:
    '''
    '''

    def __init__(self, host_address, usrname, passwd): 
        """
        Initialize the Connection to Neo4j

        Args:
            host_address: Bolt address for the Neo4j Instance
            usrname: Username for Authentication
            passwd : Password for Authentication

        Returns:
            None
        
        Raises:
            ServiceUnavailableError: When the neo4j service isn't available on the host specified
            Exception: All other address errors
        """
        try:
            self.neo = GraphDatabase.driver(uri=host_address, auth=(usrname, passwd))
        except neo4j.ServiceUnavailable as err:
            print(err,file=sys.stderr)
        except Exception as err:
            print(err,file=sys.stderr)

    def send_nodes_to_neo(
        self, 
        values, # Unique Node Values (Such as IP's)
        attribute_name = 'IP'): # Attribute Name (Can be anything) Default: IP
        
        """
        Create Nodes in the Neo4j Database

        Args:
            values:  Unique Node Values (Such as IP's)
            attribute_name: Attribute Name (Can be anything) Default: IP
        
        Raises:
            Client Errors: The Client sent a bad request - changing the request might yield a successful outcome,
            TransientErrors: The database cannot service the request right now, retrying later might yield a successful outcome.
            Database Error: The database failed to service the request.
        """
        try:
            with self.neo.session() as session:
                for val in values:
                    session.run("CREATE (n:Node{{{}:'{}'}})".format(attribute_name ,str(val)))
        except Exception as err:
            print(err, file=sys.stderr)

    def send_relations_to_neo(
        self, 
        data, # List of Two Items [Src_Attribute_Value, Dest_Attribute_Value]
        attr_one_name = 'IP', # Source Attribute Name
        attr_two_name = 'IP', # Destination Attribute Name
        relation_name = 'TO'): # Name of the Relation

        """
        Adds Relationships between two specified nodes

        Args:
            data: List of Two Items [Src_Attribute_Value, Dest_Attribute_Value]
            attr_one_name : Source Attribute Name, Default: 'IP'
            attr_two_name : Destination Attribute Name, Default: 'IP'
            relation_name : Name of the Relation, Default: 'TO'

        Returns:
            None

        Raises:
            Client Errors: The Client sent a bad request - changing the request might yield a successful outcome,
            TransientErrors: The database cannot service the request right now, retrying later might yield a successful outcome.
            Database Error: The database failed to service the request.
        """

        try:
            with self.neo.session() as session:
                for relation in data:
                    session.run("MATCH (s:Node{{{}:'{}'}}), (d:Node{{{}:'{}'}}) MERGE (s)-[:TO]->(d)".format(attr_one_name, relation[0], attr_two_name, relation[1]))
        except Exception as err:
            print(err, file=sys.stderr)

    def delete_all(self):
        """
        Clears all the data in the connected Neo4j instance. 
        
        Args:
            None
        Returns:
            None
        Raises:
            Client Errors: The Client sent a bad request - changing the request might yield a successful outcome,
            TransientErrors: The database cannot service the request right now, retrying later might yield a successful outcome.
            Database Error: The database failed to service the request.
        """
        try:
            with self.neo.session() as session:
                session.run("MATCH (n) DETACH DELETE n")
        except Exception as err:
            print(err, file=sys.stderr)

    def close(self):
        """
        Close Connecion to Neo4j

        Returns:
            None
        
        Raises:
            None
        """
        self.neo.close()
