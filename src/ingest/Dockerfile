FROM python:3.7

WORKDIR /logjam

COPY ./requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY . /logjam/ingest

ENTRYPOINT ["python3", "/logjam/ingest/scan.py"]
