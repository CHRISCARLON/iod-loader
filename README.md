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
from ImdLoader import load_with_progress

if __name__ == "__main__":
    load_with_progress()
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
