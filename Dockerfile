FROM python:3.7

WORKDIR /logjam

COPY ./src/logjam-ingest/requirements.txt /logjam/src/logjam-ingest/requirements.txt
RUN pip install -r /logjam/src/logjam-ingest/requirements.txt

COPY ./src /logjam/src

ENTRYPOINT ["python3", "/logjam/src/logjam-ingest/scan.py"]
