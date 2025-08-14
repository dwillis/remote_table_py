# Remote Table Python Library

A Python port of the Ruby gem 'remote_table'.

## Installation

```bash
uv pip install .
```

## Usage

```python
from remote_table import RemoteTable

table = RemoteTable('path_or_url_to_file')
for row in table:
    print(row)
```

## Features
- Read delimited (CSV/TSV), fixed-width, HTML, JSON, XML, YAML, ODS, XLS, XLSX files
- Local and remote file support
- Auto-detect format

## License
MIT
