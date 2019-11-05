import os

from flask import Flask, request, render_template, jsonify, json, abort
from elasticsearch import Elasticsearch

ELASTICSEARCH_HOST = os.environ.get("ELASTICSEARCH_HOST", "localhost")
ELASTICSEARCH_PORT = os.environ.get("ELASTICSEARCH_PORT", 9200)

app = Flask(__name__)
es = Elasticsearch([{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}])


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/platforms", methods=["GET"])
def get_platforms():
    return jsonify(
        [
            {"text": "vSphere", "value": "vSphere"},
            {"text": "Container Only", "value": "Container Only"},
            {"text": "StorageGRID appliance", "value": "StorageGRID appliance"},
        ]
    )


@app.route("/versions", methods=["GET"])
def get_versions():
    return jsonify(
        [
            "Pre-10.2", "10.2", "10.3", "10.4", "11.0",
            "11.1", "11.2", "11.3", "11.4",

        ]
    )


@app.route("/matchData", methods=["POST"])
def get_query():
    count = 0

    if not request.json or not "logText" in request.json:
        abort(400)
    message = request.json["logText"]

    platform = request.json.get("platform")

    version = request.json.get("storagegrid_version")

    total_hits = 0
    total_no_hits = 0

    if version:
        total_hits_q = es.search(
            index="logjam",
            _source="false",
            body={
                "min_score": "10.0",
                "aggs": {"cases": {"terms": {"field": "node_name"}}},
                "query": {
                    "bool": {
                        "should": [
                            {"match": {"message": message}},
                            {"match": {"storagegrid_version": version}},
                            # {"match": {"platform": platform}},
                        ]
                    }
                },
            },
        )

        for hits in total_hits_q["aggregations"]["cases"]["buckets"]:
            if hits["key"] == "unknown":
                total_hits += hits["doc_count"]
            else:
                total_hits += 1

        total_no_hits_q = es.search(
            index="logjam",
            _source="false",
            body={
                "aggs": {"cases": {"terms": {"field": "node_name"}}},
                "query": {
                    "bool": {
                        "should": [
                            {"match": {"message": message}},
                            {"match": {"storagegrid_version": version}},
                            # {"match": {"platform": platform}},
                        ]
                    }
                },
            },
        )

        for hits in total_no_hits_q["aggregations"]["cases"]["buckets"]:
            if hits["key"] == "unknown":
                total_no_hits += hits["doc_count"]
            else:
                total_no_hits += 1
        
        total_no_hits -= total_hits

    else:
        total_hits_q = es.search(
            index="logjam",
            _source="false",
            body={
                "min_score": "10.0",
                "aggs": {"cases": {"terms": {"field": "node_name"}}},
                "query": {
                    "bool": {
                        "should": [
                            {"match": {"message": message}},
                            # {"match": {"platform": platform}},
                        ]
                    }
                },
            },
        )

        for hits in total_hits_q["aggregations"]["cases"]["buckets"]:
            if hits["key"] == "unknown":
                total_hits += hits["doc_count"]
            else:
                total_hits += 1

        total_no_hits_q = es.search(
            index="logjam",
            _source="false",
            body={
                "aggs": {"cases": {"terms": {"field": "node_name"}}},
                "query": {
                    "bool": {
                        "should": [
                            {"match": {"message": message}},
                            # {"match": {"platform": platform}},
                        ]
                    }
                },
            },
        )

    for hits in total_no_hits_q["aggregations"]["cases"]["buckets"]:
        if hits["key"] == "unknown":
            total_no_hits += hits["doc_count"]
        else:
            total_no_hits += 1
    
    total_no_hits -= total_hits

    return jsonify(
        [
            {
                "title": "Occurances",
                "labels": ["Occurs", "Does not occur"],
                "values": [total_hits, total_no_hits],
            },
        ]
    )
