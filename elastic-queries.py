"""
This script implements a python function to return queries
"""

from elasticsearch import Elasticsearch
from datetime import datetime, date, timedelta

# Initialize elasticsearch instance
es = Elasticsearch([{'host': "34.250.xxx.xxx" , 'port': 9200}])

# A few different queries to experiment with
query = {"query": { 
           "match": {
               "High": 788.0 }}} 


query2 = { "query": {
             "range": {
                "Close": {
                         "gte": 500,
                         "lte": 750}}}}
         

query3 = {"query": {
              "range": {
                 "Date" : {
                          "gte": "2013-05-01",
                          "lte": "2014"}}}}

# Function performing search and returning formatted data                           
def get_query(es_client, body):
    res = es_client.search(index="bitcoin", size=100, body=body)
    
    print("results:")

    for hit in res['hits']['hits']: 
       print(
       {"Date": hit['_source']['Date'],
       'High': hit['_source']['High'],
       'Close': hit['_source']['Close']})
   
# Each of the queries executed
get_query(es, query)
print("\n")
get_query(es, query2)
print("\n")
get_query(es, query3)

