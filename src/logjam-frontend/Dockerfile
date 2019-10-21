FROM python:3.7-alpine

RUN pip install flask

COPY . /logjam-ui

WORKDIR /logjam-ui

ENV FLASK_APP=hello.py

CMD flask run -h 0.0.0.0
