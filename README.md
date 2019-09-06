# 2019FallTeam20

## Running ingest.py

Create virtual environment
```bash
python3 -m venv ./my-venv
source my-venv/bin/activate       # for Linux
source my-venv/Scripts/activate   # for Windows (from cmd.exe)
```

Install dependencies
```bash
pip install -r requirements.txt
```

Run ingest script on input data:
```bash
python ingest.py /path/to/input/data -v
```

Output will be created in a folder called logjam_categories.
