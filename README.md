# 2019FallTeam20

```
git clone https://github.ncsu.edu/engr-csc-sdc/2019FallTeam20.git
```

All commands are from the root of the git repository.

## Starting Elastic Search

Install docker, docker-compose, & python3-venv:
```bash
sudo apt-get install docker.io docker-compose python3-venv p7zip-full
```

Add your user to the docker group (or else run all docker commands as root which is not reccomended):
```
sudo usermod -aG docker $(whoami)
```
You may need to re-log for this to take effect.

Start services (run from project root folder):
```bash
docker-compose up -d
```
This will create a network with two containers: one for elasticsearch, and one for our frontend: logjam-ui.


## Running `ingest.py` on host machine

Create virtual environment
```bash
python3 -m venv ./my-venv
source my-venv/bin/activate
```

Install dependencies
```bash
pip install -r ./src/logjam-ingest/requirements.txt
```

Run ingest script on input data:
```bash
python ./src/logjam-ingest/ingest.py [input_directory]
```

Ingest script arguments:
```
positional arguments:
  input_directory   Directory to ingest files from

optional arguments:
  -h, --help            show this help message and exit
  --log-level LOG_LEVEL
                        log level of script: DEBUG, INFO, WARNING, or CRITICAL
  -o OUTPUT_DIRECTORY, --output-dir OUTPUT_DIRECTORY
                        Directory to output StorageGRID files to
  -s SCRATCH_SPACE, --scratch-space-dir SCRATCH_SPACE
                        Scratch space directory to unzip files into
```

The program will extract files from the input directory and insert the data into an elasticsearch index called `logjam`. Each line of log data becomes one "document" in elasticsearch.

## Retrieving Data from Elastic Search
You can verify the logjam index exists by querying the elasticsearch API on port 9200:
```bash
curl localhost:9200/_cat/indices
```

This should output something like this (index size and doc count will vary):
```
green  open .kibana_task_manager Pi8KlZsLQNuC5pQR0T25eQ 1 0        2 0 29.5kb 29.5kb
yellow open logjam               4NvFxEaZQz-gz177FVJPyQ 1 1 26129762 0 10.8gb 10.8gb
green  open .kibana_1            7wBkKNuxRRK7KEBZnxhXAw 1 0        4 1 23.9kb 23.9kb
```

You can run arbitrary searches against Elastic Search using the `_search` endpoint. For example, giving the query `node_name: phsdc-vs2` will return a selection of documents from that node.
```bash
curl 'http://sd-vm24.csc.ncsu.edu:9200/logjam/_search?pretty=true&q=node_name:phsdc-vs2'
```
Example document from response:
```json
{
        "_index" : "logjam",
        "_type" : "_doc",
        "_id" : "cT9xKG4BelPUzbw2f9oE",
        "_score" : 7.372148,
        "_source" : {
          "case" : "2006672842",
          "node_name" : "phsdc-vs2",
          "storagegrid_version" : "9.0.4-20141110.1819.ffcefb2.noarch",
          "message" : "Jan  4 01:40:25 phsdc-vs2 ADE: |12723478 11284 021521 CRMM EMVD 2017-01-04T01:40:25.274618| NOTICE   1005 CRMM: Transfer of request 42 for CBID B80DC69D5F9F57A1 (0 - 18446744073709551615) completed successfully\n",
          "platform" : "unknown",
          "categorize_time" : 1572636509660
        }
      }
```
See the [Elastic Search documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs.html) for more info.


## Accessing the UI
If you used the provided docker-compose file, the UI runs on port 80 of the host. Visit the web address of the host in your browser access the logjam ui. From here, you can paste a section of log to query for results on how frequently similar sections occur across StorageGRID nodes.


## Alternative: Running `ingest.py` with Docker
We have prepared a Dockerfile for the ingest script which can be used in place of running in a "real" Python environment. This should work but considering the effort involved with mounting volumes into the container, this method is currently not the simplest.

```bash
docker build -t logjam .
docker run -v /path/to/input:/logjam/input logjam /logjam/input
```
