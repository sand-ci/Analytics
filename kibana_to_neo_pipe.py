from itertools import chain
# import certifi
import sys

from elasticsearch import Elasticsearch
import neo4j
from neo4j import GraphDatabase

import pandas as pd
import numpy as np

import r_utils as ut

class KibanaExtractor:
    """
    This class helps move trace data from Kibana (Elastic Search) to Neo4j.
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

    def getdata(self, src_ip, dest_ip, since = 21, toDate=None, fromDate=None):
        pass
    
    def getuniquenodes(self, data=None):
        pass

    def get_paths(self, data = None, topk = None, relations = False):
        pass


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

    def close(self):
        """
        Close Connecion to Neo4j

        Returns:
            None
        
        Raises:
            None
        """
        self.neo.close()


if __name__ == "__main__":
