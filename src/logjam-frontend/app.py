from flask import (Flask, request, render_template, jsonify, json)
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


test = 0
r=es.search(index='logjam', _source="false", body={"min_score": 10.0,
    "aggs": {"cases": {"terms": {"field": "node_name.keyword"}}},
    "query": {"bool": {"should": 
        {"match": {"message": "Mar 19 11:25:51 vhacllimmgwp01 ADE: |20502835 32126 612199 HSTR %DED 2014-03-19T11:25:51.030415| NOTICE   0367 HSTR: Finished transferring successfully: bytesTransferred 430"}},
        }}})

for re in r['aggregations']['cases']['buckets']:
    if re['key'] == "unknown":
        test += re['doc_count']
    else:
        test += 1

#print(json.dumps(r))
print(test)


#@app.route("/")
#def index():
#    return render_template("index.html")

@app.route("/platforms", methods=['GET'])
def get_platforms():
    return jsonify([
        {"text": "vSphere", "value": "vSphere"},
        {"text": "Container Only", "value": "Container Only"},
        {"text": "StorageGRID appliance", "value": "StorageGRID appliance"},
        ])

@app.route("/versions", methods=['GET'])
def get_versions():
    return jsonify([
        {"text": "Pre-10.2", "value": "Pre-10.2"},
        {"text": "10.2", "value": "10.2"},
        {"text": "10.3", "value": "10.3"},
        {"text": "10.4", "value": "10.4"},
        {"text": "11.0", "value": "11.0"},
        {"text": "11.1", "value": "11.1"},
        {"text": "11.2", "value": "11.2"},
        {"text": "11.3", "value": "11.3"},
        {"text": "11.4", "value": "11.4"},
        ])


@app.route("/matchData", methods=['POST'])
def get_query():
    count = 0

    if not request.json or not 'body' in request.json:
        abort(400)
    message = request.json['body']

    platform = request.json.get('platform')
    
    version = request.json.get('storagegrid_version')

    total_hits = 0
    total_no_hits = 0

    if (version): 
        total_hits_q=es.search(index='logjam', _source="false", body={"min_score": "10.0", "aggs": {"cases": { "terms": { "field": "node_name"}}},
            "query": { "bool": { "should": [
                {"match": {"message": message}},
                {"match": {"storagegrid_version": version}},
                #{"match": {"platform": platform}},
            ]}}})

        for  hits in total_hits_q['aggregations']['cases']['buckets']:
            if hits['key'] == "unknown":
                total_hits += hits['doc_count']
            else:
                total_hits += 1
        

        total_no_hits_q=es.search(index='logjam', _source="false", body={"max_score": "10.0", "aggs": {"cases": { "terms": { "field": "node_name"}}},
            "query": { "bool": { "should": [
                {"match": {"message": message}},
                {"match": {"storagegrid_version": version}},
                #{"match": {"platform": platform}},
            ]}}})

        for  hits in total_no_hits_q['aggregations']['cases']['buckets']:                                                                                                               if hits['key'] == "unknown":                                                                                                                                                    total_no_hits += hits['doc_count']                                                                                                                                      else:                                                                                                                                                                           total_no_hits += 1   

    else:
        total_hits_q=es.search(index='logjam', _source="false", body={"min_score": "10.0", "aggs": {"cases": { "terms": { "field": "node_name"}}},
            "query": { "bool": { "should": [
                {"match": {"message": message}},
                #{"match": {"platform": platform}},
            ]}}})
            
        for  hits in total_hits_q['aggregations']['cases']['buckets']:                                                                                                                  if hits['key'] == "unknown":                                                                                                                                                    total_hits += hits['doc_count']                                                                                                                                         else:                                                                                                                                                                           total_hits += 1   
                
        total_no_hits_q=es.search(index='logjam', _source="false", body={"max_score": "10.0", "aggs": {"cases": { "terms": { "field": "node_name"}}},
            "query": { "bool": { "should": [
                {"match": {"message": message}},
                #{"match": {"platform": platform}},
            ]}}})

        for  hits in total_no_hits_q['aggregations']['cases']['buckets']:                                                                                                               if hits['key'] == "unknown":                                                                                                                                                    total_no_hits += hits['doc_count']                                                                                                                                      else:                                                                                                                                                                           total_no_hits += 1   

    
    return jsonify([
        {
            "title":"Occurances",
            "labels": ["Occurs", "Does not occur"],
            "values": [total_hits, total_no_hits]
        },
        ])

#if __name__ == "__main__":
#    app.run(host="sd-vm24.csc.ncsu.edu", port=80)

