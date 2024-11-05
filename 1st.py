from elasticsearch import Elasticsearch
import pandas as pd

# Initialize the Elasticsearch client
es = Elasticsearch("http://localhost:9200")

# a) createCollection: Creates a new index in Elasticsearch.
def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection '{p_collection_name}' created.")
    else:
        print(f"Collection '{p_collection_name}' already exists.")

# b) indexData: Indexes data from the CSV into Elasticsearch, excluding a specified column.
def indexData(p_collection_name, p_exclude_column):
    df = pd.read_csv("/mnt/data/Employee Sample Data 1.csv", encoding="latin").dropna()
    for i, row in df.iterrows():
        doc = row.to_dict()
        if p_exclude_column in doc:
            del doc[p_exclude_column]
        es.index(index=p_collection_name, id=row["Employee ID"], body=doc)
    es.indices.refresh(index=p_collection_name)
    print(f"Data indexed into '{p_collection_name}' excluding column '{p_exclude_column}'.")

# c) searchByColumn: Searches for records in the specified collection based on a column's value.
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    resp = es.search(index=p_collection_name, query={"match": {p_column_name: p_column_value}})
    for hit in resp['hits']['hits']:
        print(hit['_source'])

# d) getEmpCount: Returns the count of documents in the specified collection.
def getEmpCount(p_collection_name):
    count = es.count(index=p_collection_name)['count']
    print(f"Employee count in '{p_collection_name}': {count}")
    return count

# e) delEmpById: Deletes a document by employee ID.
def delEmpById(p_collection_name, p_employee_id):
    es.delete(index=p_collection_name, id=p_employee_id, ignore=[404])
    print(f"Employee with ID '{p_employee_id}' deleted from '{p_collection_name}'.")

# f) getDepFacet: Retrieves the count of employees grouped by department.
def getDepFacet(p_collection_name):
    resp = es.search(index=p_collection_name, size=0, aggs={
        "department_counts": {"terms": {"field": "Department.keyword"}}
    })
    for bucket in resp["aggregations"]["department_counts"]["buckets"]:
        print(f"Department: {bucket['key']}, Count: {bucket['doc_count']}")



# Variables for collection names
v_nameCollection = 'bowsiya'
v_phoneCollection = '5373'  # Replace '1234' with the last four digits of your phone number

# Execution of function calls
createCollection(v_nameCollection)
createCollection(v_phoneCollection)

getEmpCount(v_nameCollection)

indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')

getEmpCount(v_nameCollection)

delEmpById(v_nameCollection, 'E02003')

getEmpCount(v_nameCollection)

searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')

getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)


