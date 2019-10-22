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

print(count)


#@app.route("/")
#def index():
#    return render_template("index.html")

@app.route("/platforms", methods=['GET'])
def get_platforms():
    return jsonify([
        {"text": "Platform A", "value": "a"},
        {"text": "Platform B", "value": "b"},
        {"text": "Platform C", "value": "c"},
        ])

@app.route("/versions", methods=['GET'])
def get_versions():
    return jsonify([
        {"text": "1.0", "value": "1.0"},
        {"text": "2.0", "value": "2.0"},
        {"text": "3.0", "value": "3.0"},
        ])


@app.route("/charts", methods=['POST'])
def get_query():
    count = 0

    if not request.json or not 'body' in request.json:
        abort(400)
    message = request.json['body']
    platform = "None"
    version = "None"

    if 'platform' in request.json:
        platform = request.json['platform']
    
    if 'version' in request.json:
        version = request.json['version']

    res=es.search(index='logjam-test', body={"query": { "bool": { "must": [
                                                                            {"match": {"message": message}},
                                                                            {"match": {"version": version}},
                                                                            {"match": {"platform": platform}},
                                                                            ]}}})
   
    for hit in res['hits']['hits']:
            if hit['_score'] > 10:
                print(hit['_score'])
                count = count + 1

    print(count)

#if __name__ == "__main__":
#    app.run(host="sd-vm24.csc.ncsu.edu", port=80)

