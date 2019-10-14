FROM python:3.7

WORKDIR /logjam

COPY ./src/ingester/requirements.txt /logjam/src/ingester/requirements.txt
RUN pip install -r /logjam/src/ingester/requirements.txt

COPY ./src /logjam/src

ENTRYPOINT ["python3", "/logjam/src/inester/ingest.py"]
