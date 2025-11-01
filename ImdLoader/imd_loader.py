"""
IMD Loader - Python interface for loading IMD  data into a local DuckDB.

Usage:
    from ImdLoader import imd_data_loader

    for status in imd_data_loader():
        print(status)
"""

from pathlib import Path
from typing import Generator, Tuple, List, Dict, Any
import requests
from bs4 import BeautifulSoup
import pandas as pd
import duckdb
import re


# Config
IMD_2025_URL = (
    "https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025"
)
BASE_URL = "https://assets.publishing.service.gov.uk"
DEFAULT_DATA_DIR = Path("data")
DEFAULT_DB_PATH = Path("IMD2025.duckdb")


# Custom Exceptions
class IMDLoaderError(Exception):
    """Base exception for IMD Loader errors."""

    pass


class DownloadError(IMDLoaderError):
    """Error downloading files."""

    pass


class ExtractionError(IMDLoaderError):
    """Error extracting download links."""

    pass


class LoadError(IMDLoaderError):
    """Error loading data into database."""

    pass


def imd_data_loader(
    data_dir: Path = DEFAULT_DATA_DIR,
    db_path: Path = DEFAULT_DB_PATH,
    url: str = IMD_2025_URL,
) -> Generator[Dict[str, Any], None, None]:
    """
    Main loader function.

    Downloads and loads all IMD 2025 files into DuckDB.

    Yields status dictionaries with progress information.

    Args:
        data_dir: Directory to store downloaded files
        db_path: Path to DuckDB database
        url: URL to fetch download links from

    Yields:
        Dict containing status information:
            {'stage': 'extracting'|'downloading'|'loading'|'complete', ...}
    """

    def sanitise_name(name: str | int) -> str:
        """Sanitise names for use as schema/table identifiers."""
        sanitised = (
            str(name)
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace("&", "and")
            .replace("(", "")
            .replace(")", "")
        )
        sanitised = re.sub(r"_+", "_", sanitised)
        return sanitised.strip("_")

    def get_download_links(page_url: str) -> List[Dict[str, str]]:
        """Extract download links from the IMD 2025 Gov UK page."""
        try:
            response = requests.get(page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            attachments = soup.find_all("section", class_="gem-c-attachment")

            files = []
            for attachment in attachments:
                link = attachment.find("a", class_="govuk-link")
                if not link or "href" not in link.attrs:
                    continue

                href = str(link["href"])
                if not (href.endswith(".xlsx") or href.endswith(".csv")):
                    continue

                if href.startswith("/media/"):
                    href = BASE_URL + href
                elif not href.startswith("http"):
                    continue

                files.append(
                    {
                        "url": href,
                        "filename": href.split("/")[-1],
                    }
                )

            return files
        except Exception as e:
            raise ExtractionError(f"Failed to extract download links: {e}") from e

    def download_file_gen(
        files: List[Dict[str, str]], output_dir: Path
    ) -> Generator[Tuple[str, Path], None, None]:
        """
        Generator that downloads files and yields (status, path) tuples.
        """
        output_dir.mkdir(exist_ok=True, parents=True)

        for file_info in files:
            url = file_info["url"]
            filename = file_info["filename"]
            file_path = output_dir / filename

            if file_path.exists():
                yield ("exists", file_path)
                continue

            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()

                with open(file_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                yield ("downloaded", file_path)
            except Exception:
                yield ("failed", file_path)

    def process_excel_file(
        excel_path: Path,
    ) -> Generator[Tuple[str, str, pd.DataFrame], None, None]:
        """
        Generator that yields (schema_name, table_name, dataframe) for each sheet.
        Skips 'Notes' sheets.
        """
        schema_name = sanitise_name(excel_path.stem)

        try:
            xl_file = pd.ExcelFile(excel_path)

            for sheet_name in xl_file.sheet_names:
                if sheet_name == "Notes":
                    continue

                try:
                    df = pd.read_excel(excel_path, sheet_name=sheet_name)

                    if len(df.columns) == 0 or len(df) == 0:
                        continue

                    table_name = sanitise_name(sheet_name)
                    yield (schema_name, table_name, df)
                except Exception:
                    continue

        except Exception:
            pass

    def process_csv_file(
        csv_path: Path,
    ) -> Generator[Tuple[str, str, pd.DataFrame], None, None]:
        """
        Generator that yields (schema_name, table_name, dataframe) for CSV file.
        """
        schema_name = sanitise_name(csv_path.stem)
        table_name = sanitise_name(csv_path.stem)

        try:
            df = pd.read_csv(csv_path)

            if len(df.columns) == 0 or len(df) == 0:
                return

            yield (schema_name, table_name, df)
        except Exception:
            pass

    def load_to_duckdb(
        schema_name: str, table_name: str, df: pd.DataFrame, db_path: Path
    ) -> bool:
        """Load a dataframe into DuckDB. Returns True on success."""
        try:
            con = duckdb.connect(str(db_path))
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            full_table_name = f"{schema_name}.{table_name}"
            con.execute(
                f"CREATE OR REPLACE TABLE {full_table_name} AS SELECT * FROM df"
            )
            con.close()
            return True
        except Exception:
            return False

    yield {"stage": "extracting", "message": "Fetching download links"}

    try:
        files = get_download_links(url)
    except ExtractionError as e:
        yield {"stage": "error", "message": str(e)}
        return

    yield {
        "stage": "downloading",
        "message": f"Found {len(files)} files",
        "total_files": len(files),
    }

    data_files = []
    for status, file_path in download_file_gen(files, data_dir):
        yield {
            "stage": "downloading",
            "status": status,
            "file": file_path.name,
        }
        if file_path.suffix in (".xlsx", ".csv"):
            data_files.append(file_path)

    yield {
        "stage": "loading",
        "message": f"Loading {len(data_files)} files into database",
        "total_files": len(data_files),
    }

    total_tables = 0
    for data_file in data_files:
        yield {"stage": "loading", "file": data_file.name, "status": "processing"}

        tables_loaded = 0

        # Choose processor based on file type
        if data_file.suffix == ".xlsx":
            processor = process_excel_file(data_file)
        elif data_file.suffix == ".csv":
            processor = process_csv_file(data_file)
        else:
            continue

        for schema_name, table_name, df in processor:
            success = load_to_duckdb(schema_name, table_name, df, db_path)
            if success:
                tables_loaded += 1
                total_tables += 1
                yield {
                    "stage": "loading",
                    "file": data_file.name,
                    "table": f"{schema_name}.{table_name}",
                    "rows": len(df),
                    "status": "loaded",
                }

        yield {
            "stage": "loading",
            "file": data_file.name,
            "tables_loaded": tables_loaded,
            "status": "complete",
        }

    yield {
        "stage": "complete",
        "total_tables": total_tables,
        "database": str(db_path.absolute()),
    }


def load_with_progress(
    data_dir: Path = DEFAULT_DATA_DIR,
    db_path: Path = DEFAULT_DB_PATH,
    url: str = IMD_2025_URL,
) -> int:
    """
    Run the loader with formatted progress output.

    Args:
        data_dir: Directory to store downloaded files
        db_path: Path to DuckDB database
        url: URL to fetch download links from

    Returns:
        int: Total number of tables loaded
    """
    total_tables = 0

    for status in imd_data_loader(data_dir, db_path, url):
        stage = status.get("stage")

        if stage == "extracting":
            print(f"{status['message']}...")

        elif stage == "downloading":
            if "total_files" in status:
                print(f"\n{status['message']}")
            elif status.get("status") == "downloaded":
                print(f"  ✓ {status['file']}")
            elif status.get("status") == "exists":
                print(f"  ⊘ {status['file']} (exists)")

        elif stage == "loading":
            if "total_files" in status:
                print(f"\n{status['message']}")
            elif status.get("status") == "processing":
                print(f"\n  {status['file']}")
            elif status.get("status") == "loaded":
                print(f"    ✓ {status['table']} ({status['rows']} rows)")

        elif stage == "complete":
            total_tables = status["total_tables"]
            print(f"\n✓ Loaded {total_tables} tables into {status['database']}")
            print("Quack quack 🦆")

    return total_tables


def list_tables(db_path: Path = DEFAULT_DB_PATH) -> List[str]:
    """List all tables in the database."""
    con = duckdb.connect(str(db_path))
    query = """
        SELECT table_schema || '.' || table_name as full_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """
    tables = con.execute(query).fetchall()
    con.close()
    return [table[0] for table in tables]


def query(sql: str, db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
    """Execute a SQL query against the database."""
    con = duckdb.connect(str(db_path))
    result = con.execute(sql).df()
    con.close()
    return result
