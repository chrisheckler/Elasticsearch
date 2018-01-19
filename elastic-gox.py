"""
This scripts takes MTGOXUSD.csv which has about 1300 rows of data and indexes into Elasticsearch.
"""

from elasticsearch import Elasticsearch, helpers
import csv

# Initiate elasticsearch instance
csvfile = 'MTGOXUSD.csv'
es = Elasticsearch([{'host': "54.91.101.130", 'port': 9200}])

# Create mapping for elasticsearch index
request_body = {
	    "settings" : {
	        "number_of_shards": 5,
	        "number_of_replicas": 1
	    },
	    "mappings": {
                "crypto-curr": { 
                    "properties": { 
                       "Date":   {  "type": "date"},
                       "Open":    { "type": "float"  }, 
                       "High":     { "type": "float"  }, 
                       "Low":      { "type": "float" },  
                       "Close":      { "type": "float" },  
                       "Volume(BTC)":      { "type": "float" },  
                       "Volume(Currency)":      { "type": "float" },  
                       "Weighted Price":      { "type": "float" }  
                     }
                 }
            }
        }
	
# Check if prior index 'bitcoin' exists, if so delete
if es.indices.exists('bitcoin'):
    print("deleting {} index...".format('bitcoin'))
    res = es.indices.delete(index = 'bitcoin')
    print(" response: {} ".format(res))

# Create new 'bitcoin' index with above mapping
print("creating 'bitcoin' index...")
es.indices.create(index = 'bitcoin', body = request_body)

with open(csvfile, 'r') as csvdata:
    reader =  csv.DictReader(csvdata, delimiter=",")
    helpers.bulk(es, reader, index='bitcoin', doc_type='crypto')    
            
 # Returns the first 5 rows as a test.
print("searching...")
res = es.search(index = 'bitcoin', size=5, body={"query": {"match_all": {}}})
print(" response: {}".format(res))

print("results:")
for hit in res['hits']['hits']:
    print(hit["_source"])

      
           
