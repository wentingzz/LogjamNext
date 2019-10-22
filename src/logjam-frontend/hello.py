from flask import (Flask, render_template, request, jsonify)

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/platforms", methods=["GET"])
def get_platforms():
    return jsonify([
        {"text": "Platform A", "value": "a"},
        {"text": "Platform B", "value": "b"},
        {"text": "Platform C", "value": "c"},
        {"text": "Platform D", "value": "d"},
        ])

@app.route("/versions", methods=["GET"])
def get_versions():
    return jsonify([
        {"text": "1.0", "value": "1.0"},
        {"text": "1.1.4", "value": "1.1.4"},
        {"text": "2.3", "value": "2.3"},
        ])

@app.route("/occurrences", methods=["POST"])
def get_occurrences():
    log_text = request.json["logText"]

    app.logger.info(log_text)

    return jsonify([
        {
            "title": "Occurrences across nodes",
            "labels": ["Occurs", "Does not occur"],
            "values": [57, 43]
        },
        {
            "title": "Occurrences by version",
            "labels": ["1.0", "2.1.3", "2.2.4"],
            "values": [22, 13, 44]
        },
        {
            "title": "Occurrences by platform",
            "labels": ["A", "B", "C"],
            "values": [3, 4, 5]
        },
        ])
