FROM python:3.7

WORKDIR /logjam

COPY ./src/ingest/requirements.txt /logjam/src/ingest/requirements.txt
RUN pip install -r /logjam/src/ingest/requirements.txt

COPY ./src /logjam/src

ENTRYPOINT ["python3", "/logjam/src/ingest/scan.py"]
