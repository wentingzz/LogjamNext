FROM python:3.7-alpine

RUN pip install flask gunicorn

COPY . /logjam-ui

WORKDIR /logjam-ui

CMD gunicorn hello:app -b 0.0.0.0:8080 --log-level DEBUG
