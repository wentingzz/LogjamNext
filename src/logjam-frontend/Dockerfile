FROM python:3.7-alpine

# Install python dependencies
RUN pip install flask gunicorn

COPY . /logjam-ui

WORKDIR /logjam-ui

# Download external JS and CSS
RUN python ./fetch_scripts.py

CMD gunicorn hello:app -b 0.0.0.0:8080 --log-level DEBUG
