from flask import (Flask, request, render_template, jsonify)
from elasticsearch import Elasticsearch

app = Flask(__name__)
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

res=es.search(index='logjam', body={"query": { "bool": { "must": [
    {"match": {"message": "Mar 19 11:25:51 vhacllimmgwp01 ADE: |20502835 32126 612199 HSTR %DED 2014-03-19T11:25:51.030415| NOTICE   0367 HSTR: Finished transferring successfully: bytesTransferred 430"}},
                                                                        #{"match": {"version": "version"}},
                                                                        #{"match": {"platform": "platform"}},
                                                                      ]}}})


count = 0

for hit in res['hits']['hits']:
    if hit['_score'] > 10:
        print(hit['_score'])
        count = count + 1


resc=es.count(index='logjam', body={"query": { "bool": { "must": [
        {"match": {"message": "Mar 19 11:25:51 vhacllimmgwp01 ADE: |20502835 32126 612199 HSTR %DED 2014-03-19T11:25:51.030415| NOTICE   0367 HSTR: Finished transferring successfully: bytesTransferred 430"}},
 ]}}})

print(count)
print(resc)


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

    res=es.count(index='logjam', body={"query": { "bool": { "must": [
                                                                            {"match": {"message": message}},
                                                                            {"match": {"storagegrid_version": version}},
                                                                            {"match": {"platform": platform}},
                                                                            ]}}})
   
    print(res)
    print(count)

#if __name__ == "__main__":
#    app.run(host="sd-vm24.csc.ncsu.edu", port=80)

