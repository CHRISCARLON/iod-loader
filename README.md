# IMD 2025 Data Loader 🦆

A simple Python package for downloading and loading the latest English Indices of Deprivation 2025 data into DuckDB.

Source Data -> [Click Here](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025)

## Installation

```bash
pip install imd-loader
```

Or with uv:

```bash
uv add imd-loader
```

## Quick Start

### Simple Usage

```python
from ImdLoader import imd_data_loader

# Load all IMD 2025 data
for status in imd_data_loader():
    if status['stage'] == 'complete':
        print(f"Loaded {status['total_tables']} tables!")
```

## Database Structure

The loader creates a DuckDB database with the following structure:

- **Schemas**: Named after the Excel filename
- **Tables**: Named after the Excel sheet names
- **Notes sheets**: Automatically skipped

## Query Data

```sql
SELECT *
FROM File_2_IoD2025_Domains_of_Deprivation.IoD2025_Domains;
```
