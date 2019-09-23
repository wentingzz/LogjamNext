FROM python:3.7

WORKDIR /logjam

COPY requirements.txt /logjam
RUN pip install -r requirements.txt


COPY logjam /logjam

RUN python setup.py

ENTRYPOINT ["python3", "/logjam/ingest.py"]
