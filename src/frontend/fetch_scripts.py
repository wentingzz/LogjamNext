"""
@author Jeremy Schmidt

Download external JavaScript and CSS for hosting
"""
import os.path
from urllib.request import urlretrieve

JS_FOLDER = "static/js/"
CSS_FOLDER = "static/css/"

js_scripts = [
    # Vue.js - Used for connecting html elements to js code
    "https://cdn.jsdelivr.net/npm/vue@2.6.0/dist/vue.min.js",
    # Provides async http requests for Vue
    "https://cdn.jsdelivr.net/npm/vue-resource@1.5.1/dist/vue-resource.min.js",
    # Chart.js - Easy, good looking charts
    "https://cdn.jsdelivr.net/npm/chart.js@2.9.1/dist/Chart.min.js",
    # JS component for Bootstrap CSS
    "https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js",
    # JQuery - Required for Bootstrap
    "https://code.jquery.com/jquery-3.3.1.slim.min.js",
    ]

css_files = [
    # Bootstrap.css - provides easy styling and layout based on html classes
    "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css",
    ]


def download_files(url_list, dest_folder):
    """ Download each file in the list to the destination folder """
    for file_url in url_list:
        base_name = file_url.split("/")[-1]
        destination = os.path.join(dest_folder, base_name)
        print("Downloading %s to %s" % (file_url, destination))
        urlretrieve(file_url, destination)


if __name__ == "__main__":
    # Download all JavaScript
    download_files(js_scripts, JS_FOLDER)
    # Download all CSS
    download_files(css_files, CSS_FOLDER)
