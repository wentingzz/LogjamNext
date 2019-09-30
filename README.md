# 2019FallTeam20

## Starting ELK Stack

Install docker and docker-compose:
```bash
sudo apt-get install docker docker-compose
```

Start services:
```bash
docker-compose up -d
```
This will create a network with three containers: one logstash, one elasticsearch, and one kibana.

Logstash will start with a pipeline located at `logjam/logstash/logstash.conf`. This pipeline is configured to read from `logjam_categories` and output to an elasticsearch index called `logjam-test`.

## Running ingest.py

Create virtual environment
```bash
python3 -m venv ./my-venv
source my-venv/bin/activate       # for Linux
source my-venv/Scripts/activate   # for Windows (from git-bash.exe)
my-venv\Scripts\activate.bat      # for Windows (from cmd.exe)
```

Install dependencies
```bash
pip install -r requirements.txt
```

Run ingest script on input data:
```bash
python ingest.py /path/to/input/data
```

Ingest script arguments:
```
positional arguments:
  ingestion_directory   Directory to ingest files from

optional arguments:
  -h, --help            show this help message and exit
  --log-level LOG_LEVEL
                        log level of script: DEBUG, INFO, WARNING, or CRITICAL
  -o OUTPUT_DIRECTORY, --output-dir OUTPUT_DIRECTORY
                        Directory to output StorageGRID files to
  -s SCRATCH_SPACE, -scratch-space-dir SCRATCH_SPACE
                        Scratch space directory to unzip files into
```

By default, output will be created in a folder called logjam_categories. If you started ELK services with the provided docker-compose file, this should already go to the same directory that logstash is watching.

## Alternative: Running ingest.py with Docker
We have prepared a Dockerfile for the ingest script which can be used in place of running in a "real" Python environment. This should work but considering the effort involved with mounting volumes into the container, this method is currently not the simplest.

```bash
docker build -t logjam .
docker run -v /path/to/input:/logjam_input logjam /logjam_input
```
