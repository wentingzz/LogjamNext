FROM python:3.7-alpine

# Install python dependencies
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY . /logjam-ui

WORKDIR /logjam-ui

# Download external JS and CSS
RUN python ./fetch_scripts.py

CMD gunicorn app:app -b 0.0.0.0:8080 --log-level DEBUG
