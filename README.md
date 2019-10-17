# 2019FallTeam20

```
git clone https://github.ncsu.edu/engr-csc-sdc/2019FallTeam20.git
```

All commands will be run from the root of the git repository.

## Starting ELK Stack

Install docker, docker-compose, & python3-venv:
```bash
sudo apt-get install docker.io docker-compose python3-venv p7zip-full
```

Add your user to the docker group (or else run all subsequent docker commands as root which is not reccomended):
```
sudo usermod -aG docker $(whoami)
```
You may need to re-log for this to take effect.

Start services (run from project root folder):
```bash
docker-compose up -d
```
This will create a network with three containers: one logstash, one elasticsearch, and one kibana.

Logstash will start with a pipeline located at `./src/logstash/logstash.conf`. This pipeline is configured to read from `./data/logjam-categories` and output to an elasticsearch index called `logjam`.

## Running `ingest.py` on host machine

Create virtual environment
```bash
python3 -m venv ./my-venv
source my-venv/bin/activate       # for Linux
source my-venv/Scripts/activate   # for Windows (from git-bash.exe)
my-venv\Scripts\activate.bat      # for Windows (from cmd.exe)
```

Install dependencies
```bash
pip install -r ./src/ingester/requirements.txt
```

Run ingest script on input data:
```bash
python ./src/ingester/ingest.py [input_directory]
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

By default, output will be created in a folder called `./data/logjam-categories`. If you started ELK services with the provided `docker-compose.yml` file, this should already go to the same directory that logstash is watching.

## Retrieving Data from Elastic Search
The data will be sent by logstash to an elasticsearch index called `logjam`. Check that the index exists and has data by querying the Elastic Search API:
```bash
curl localhost:9200/_cat/indices
```

Should output something like this (index size and doc count will vary):
```
green  open .kibana_task_manager Pi8KlZsLQNuC5pQR0T25eQ 1 0        2 0 29.5kb 29.5kb
yellow open logjam               4NvFxEaZQz-gz177FVJPyQ 1 1 26129762 0 10.8gb 10.8gb
green  open .kibana_1            7wBkKNuxRRK7KEBZnxhXAw 1 0        4 1 23.9kb 23.9kb
```

Arbitrary searches can be run against Elastic Search using the `_search` endpoint:
```
curl 'localhost:9200/logjam/_search?pretty=true&q=category:"bycast"'
```
See the [Elastic Search documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs.html) for more info.

## Accessing Kibana

Kibana should be accessible on port `5601` of the host machine. Kibana should be able to connect to the Elastic Search index but as yet we have not created any Kibana-specific configuration.

## Alternative: Running `ingest.py` with Docker
We have prepared a Dockerfile for the ingest script which can be used in place of running in a "real" Python environment. This should work but considering the effort involved with mounting volumes into the container, this method is currently not the simplest.

```bash
docker build -t logjam .
docker run -v /path/to/input:/logjam/input logjam /logjam/input
```
