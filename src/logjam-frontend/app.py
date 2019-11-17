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
            body={
                "aggs":                             # aggregate our query below
                {
                    "by_nodename_and_casenumber":   # name of aggregation
                    {
                        "composite":                # composite combines two fields
                        {
                            "size" : "10000",       # forces composite to return all pairs
                            "sources":
                            [                       # all pairs of nodename and casenum
                                { "sorting_by_the_node" : {"terms":{"field":"node_name"}} },
                                { "sorting_by_the_case" : {"terms":{"field":"case"}} }
                            ]
                        }
                    }
                },
                
                "query":                            # query that will be aggregated
                {
                    "bool":                         # use bool query type
                    {
                        "filter":                   # filter = include/exclude docs based
                        [                           # on condition, score not included
                            {
                                "term":             # term query matches a token exactly
                                {                   # (not fuzzy) to a field
                                    "storagegrid_version":
                                    {               # exact version match
                                        "value" : version
                                    }               # replace with a 'range query' soon
                                }
                            },                            
                            # {
                                # "term":           # term query matches a token exactly
                                # {                 # (not fuzzy) to a field
                                    # "platform":
                                    # {             # exact platform match
                                        # "value" : platform
                                    # }
                                # }
                            # },
                        ],
                        
                        "should":                   # should = OR w/ a summed score
                        [                           # array = multiple clauses can be used
                            {
                                "match":            # match query searches all tokens from
                                {                   # message anywhere in doc text, we set
                                    "message":      # the minimum limit to match = 50%
                                    {
                                        "query" : message,
                                        "auto_generate_synonyms_phrase_query" : "false",
                                        "fuzziness" : "0",
                                        "max_expansions" : "1",
                                        "lenient" : "false",
                                        "operator" : "OR",
                                        "minimum_should_match" : "50%",
                                        "zero_terms_query" : "none"
                                    }
                                }
                            },
                        ]
                    }
                },
                "size" : "0"                        # no docs returned, just aggregation
            },
        )
        total_hits = len(total_hits_q["aggregations"]["by_nodename_and_casenumber"]["buckets"])

        total_all_q = es.search(
            index="logjam",
            body={
                "aggs":                             # aggregate our query below
                {
                    "by_nodename_and_casenumber":   # name of aggregation
                    {
                        "composite":                # composite combines two fields
                        {
                            "size" : "10000",       # forces composite to return all pairs
                            "sources":
                            [                       # all pairs of nodename and casenum
                                { "sorting_by_the_node" : {"terms":{"field":"node_name"}} },
                                { "sorting_by_the_case" : {"terms":{"field":"case"}} }
                            ]
                        }
                    }
                },
                
                "query":                            # query that will be aggregated
                {
                    "bool":                         # use bool query type
                    {
                        "filter":                   # filter = include/exclude docs based
                        [                           # on condition, score not included
                            {
                                "term":             # term query matches a token exactly
                                {                   # (not fuzzy) to a field
                                    "storagegrid_version":
                                    {               # exact version match
                                        "value" : version
                                    }               # replace with a 'range query' soon
                                }
                            },
                            # {
                                # "term":           # term query matches a token exactly
                                # {                 # (not fuzzy) to a field
                                    # "platform":
                                    # {             # exact platform match
                                        # "value" : platform
                                    # }
                                # }
                            # },
                        ],
                        
                        "should":                   # should = OR w/ a summed score
                        [                           # array = multiple clauses can be used
                            {
                                "match_all":        # match all query returns all docs
                                {                   # with a score of 1.0
                                
                                }    
                            }
                        ]
                    }
                },
                "size" : "0"                        # no docs returned, just aggregation
            },
        )
        total_all = len(total_all_q["aggregations"]["by_nodename_and_casenumber"]["buckets"])
        
        total_no_hits = total_all - total_hits

    else:
        total_hits_q = es.search(
            index="logjam",
            _source="false",
            body={
                "aggs":                             # aggregate our query below
                {
                    "by_nodename_and_casenumber":   # name of aggregation
                    {
                        "composite":                # composite combines two fields
                        {
                            "size" : "10000",       # forces composite to return all pairs
                            "sources":
                            [                       # all pairs of nodename and casenum
                                { "sorting_by_the_node" : {"terms":{"field":"node_name"}} },
                                { "sorting_by_the_case" : {"terms":{"field":"case"}} }
                            ]
                        }
                    }
                },
                
                "query":                            # query that will be aggregated
                {
                    "bool":                         # use bool query type
                    {
                        "should":                   # should = OR w/ a summed score
                        [                           # array = multiple clauses can be used
                            {
                                "match":            # match query searches all tokens from
                                {                   # message anywhere in doc text, we set
                                    "message":      # the minimum limit to match = 50%
                                    {
                                        "query" : message,
                                        "auto_generate_synonyms_phrase_query" : "false",
                                        "fuzziness" : "0",
                                        "max_expansions" : "1",
                                        "lenient" : "false",
                                        "operator" : "OR",
                                        "minimum_should_match" : "50%",
                                        "zero_terms_query" : "none"
                                    }
                                }
                            },
                        ]
                    }
                },
                "size" : "0"                        # no docs returned, just aggregation
            },
        )
        # val_to_print = json.dumps(total_hits_q, indent=4)
        # logging.critical(str(val_to_print))
        total_hits = len(total_hits_q["aggregations"]["by_nodename_and_casenumber"]["buckets"])

        total_all_q = es.search(
            index="logjam",
            body={
                "aggs":                             # aggregate our query below
                {
                    "by_nodename_and_casenumber":   # name of aggregation
                    {
                        "composite":                # composite combines two fields
                        {
                            "size" : "10000",       # forces composite to return all pairs
                            "sources":
                            [                       # all pairs of nodename and casenum
                                { "sorting_by_the_node" : {"terms":{"field":"node_name"}} },
                                { "sorting_by_the_case" : {"terms":{"field":"case"}} }
                            ]
                        }
                    }
                },
                
                "query":                            # query that will be agregated
                {
                    "match_all" : {}                # all docs considered/matched
                },
                "size" : "0"                        # no docs returned, just aggregation
            }
        )
        # val_to_print = json.dumps(total_all_q, indent=4)
        # logging.critical(str(val_to_print))
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
