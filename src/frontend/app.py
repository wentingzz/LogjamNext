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
            "Unknown",
        ]
    )


@app.route("/versions", methods=["GET"])
def get_versions():
    return jsonify(
        [
            "Pre-10", "10.0", "10.1", "10.2", "10.3", "10.4", "11.0",
            "11.1", "11.2", "11.3", "11.4","Unknown",

        ]
    )


@app.route("/matchData", methods=["POST"])
def get_query():
    if not request.json or not "logText" in request.json:
        abort(400)
    
    total_hits=0
    total_no_hits=0
    version_values=[]
    version_labels=[]
    platform_labels=[]
    platform_values=[]

    chart_list = []
   
    request_body = { 
            "aggs":{
                "by_node":{
                    "terms":{
                        "size":"10000",
                        "field":"node_name"
                    },
                }
            },
            "query":{ "bool":{ "filter":[], "must":[{"match_all":{}}],}},
            "size":"0"}

    message = request.json["logText"]

    platform = request.json.get("platform")

    version = request.json.get("sgVersion")
    
    message_query = {
        "match":{"message":{
            "query":message,
            "auto_generate_synonyms_phrase_query":"false",
            "fuzziness":0,
            "max_expansions":1,
            "minimum_should_match":"75%",
        }}
    }

    if version == "Pre-10":
        request_body["query"]["bool"]["filter"].append(
            {"range":{"major_version":{"gt":0,"lt":10}}})

    elif version and version != "All Versions":
        if version == "Unknown":
            major_version=-1
            minor_version=-1
        else:
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

    request_body["query"]["bool"]["must"]=message_query
    
    total_hits_q = es.search(
        index="logjam",
        body=request_body)
        
    total_hits = len(total_hits_q["aggregations"]["by_node"]["buckets"])
    
    total_all = len(total_all_q["aggregations"]["by_node"]["buckets"])
    
    total_no_hits = total_all - total_hits
  
    chart_list.append({
        "title":"Occurances",
        "labels": ["Occurs", "Does not occur"],
        "values": [total_hits, total_no_hits]
    })
   
    if version == "All Versions":
        request_body["aggs"]={
            "by_version":{
                "terms":{
                    "size":10000,
                    "field":"major_version"
                },
                "aggs":{
                    "by_node":{"terms":{"field":"node_name"}}
                }
            }
        }
        version_q=es.search(
            index="logjam",
            body=request_body)

        version_buckets=version_q["aggregations"]["by_version"]["buckets"]
        for bucket in version_buckets:
            version_label = bucket["key"]
            if version_label == -1:
                version_label = "Unknown"
            version_labels.append(version_label)
            version_values.append(len(bucket["by_node"]["buckets"]))
        
        if version_values:
            chart_list.append({
                "title":"Occurances by Version",
                "labels":version_labels,
                "values":version_values
            })

    if platform == "All Platforms":
        request_body["aggs"]={
            "by_platform":{
                "terms":{
                    "size":10000,
                    "field":"platform"
                },
                "aggs":{
                    "by_node":{"terms":{"field":"node_name"}}
                }
            }
        }

        platform_q=es.search(
            index="logjam",
            body=request_body)
        
        platform_buckets=platform_q["aggregations"]["by_platform"]["buckets"]
        for bucket in platform_buckets:
            platform_labels.append(bucket["key"])
            platform_values.append(len(bucket["by_node"]["buckets"]))

        if platform_values:
            chart_list.append({
                "title":"Occurances by Platform",
                "labels":platform_labels,
                "values":platform_values
            })

    return jsonify(chart_list)
