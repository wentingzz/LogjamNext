from flask import (Flask, render_template, request, jsonify)

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

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
        }])
