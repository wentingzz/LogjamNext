"""
@author Jeremy Schmidt
@author Daniel Grist
@author Nathaniel Brooks

Basic Flask app for serving our webpage and ferrying queries to Elasticsearch
"""


import os
import logging
import sys
import json

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
            "vSphere",
            "Container",
            "StorageGRID appliance",
        ]
    )


@app.route("/versions", methods=["GET"])
def get_versions():
    return jsonify(
        [
            "Pre-10", "10.0", "10.1", "10.2", "10.3", "10.4", "11.0",
            "11.1", "11.2", "11.3", "11.4",

        ]
    )


@app.route("/matchData", methods=["POST"])
def get_query():
    if not request.json or not "logText" in request.json:
        abort(400)

    total_hits=0
    total_no_hits=0

    request_body = { 
            "aggs":{
                "by_nodename_and_casenumber":{ "composite":{
                    "size":"10000",
                    "sources":[
                        {"sorting_by_node":{"terms":{"field":"node_name.keyword"}}},
                        {"sorting_by_case":{"terms":{"field":"case.keyword"}}}
                    ]
                }}
            },
            "query":{ "bool":{ "filter":[], "must":[{"match_all":{}}],}},
            "size":"0"}

    message = request.json["logText"]

    platform = request.json.get("platform")

    version = request.json.get("sgVersion")
    
    if version == "Pre-10":
        request_body["query"]["bool"]["filter"].append(
            {"range":{"major_version":{"gt":0,"lt":10}}})

    elif version and version != "All Versions":
        v=version.split('.')
        major_version=v[0]
        minor_version=v[1]
        request_body["query"]["bool"]["filter"].append(
            {"term":{"major_version":{"value":major_version}}})
        request_body["query"]["bool"]["filter"].append(
            {"term":{"minor_version":{"value":minor_version}}})

    if platform != "All Platforms":
        if platform == "StorageGRID appliance":
            platform = "SGA"
        request_body["query"]["bool"]["filter"].append(
            {"term":{"platform":{"value":platform}}})

    total_all_q= es.search(
        index="logjam",    
        body=request_body)

    request_body["query"]["bool"]["must"]={
        "match":{ "message":{
            "query":message,
            "auto_generate_synonyms_phrase_query":"false",
            "fuzziness":"0",
            "max_expansions":"1",
            "minimum_should_match":"75%",
        }}
    }
    total_hits_q = es.search(
        index="logjam",
        body=request_body)
        
    total_hits = len(total_hits_q["aggregations"]["by_nodename_and_casenumber"]["buckets"])
    
    total_all = len(total_all_q["aggregations"]["by_nodename_and_casenumber"]["buckets"])
    
    total_no_hits = total_all - total_hits
    
    return jsonify(
        [
            {
                "title": "Occurances",
                "labels": ["Occurs", "Does not occur"],
                "values": [total_hits, total_no_hits],
            },
        ]
    )
