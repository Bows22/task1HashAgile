from elasticsearch import Elasticsearch

# Initialize Elasticsearch client
es = Elasticsearch("http://localhost:8989")

# Get cluster information
info = es.info()
print(info)
