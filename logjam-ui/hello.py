from flask import (Flask, url_for, redirect)

app = Flask(
        __name__,
        static_folder="./static"
)

@app.route("/")
def index():
    return redirect(url_for("static", filename="index.html"))
