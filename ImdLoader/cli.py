"""
CLI interface for IMD Loader.

Usage:
    imd load                              # Load IMD data into DuckDB
    imd list-tables                       # List all tables in the database
    imd query "SELECT * FROM table"       # Execute a SQL query
"""

import argparse
import sys
from pathlib import Path
from ImdLoader.imd_loader import (
    load_with_progress,
    list_tables,
    query,
    DEFAULT_DATA_DIR,
    DEFAULT_DB_PATH,
)


def cmd_load(args):
    """Load IMD data into DuckDB."""
    data_dir = Path(args.data_dir) if args.data_dir else DEFAULT_DATA_DIR
    db_path = Path(args.db_path) if args.db_path else DEFAULT_DB_PATH

    try:
        load_with_progress(data_dir=data_dir, db_path=db_path)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_list_tables(args):
    """List all tables in the database."""
    db_path = Path(args.db_path) if args.db_path else DEFAULT_DB_PATH

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        print("Run 'imd load' first to create the database.", file=sys.stderr)
        return 1

    try:
        tables = list_tables(db_path=db_path)
        if not tables:
            print("No tables found in database.")
        else:
            print(f"Found {len(tables)} tables:\n")
            for table in tables:
                print(f"  {table}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_query(args):
    """Execute a SQL query against the database."""
    db_path = Path(args.db_path) if args.db_path else DEFAULT_DB_PATH

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}", file=sys.stderr)
        print("Run 'imd load' first to create the database.", file=sys.stderr)
        return 1

    try:
        result = query(args.sql, db_path=db_path)
        print(result)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="imd",
        description="IMD 2025 Data Loader - Download and load English Indices of Deprivation data into DuckDB",
    )

    # Global options
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Load command
    load_parser = subparsers.add_parser(
        "load",
        help="Download and load IMD 2025 data into DuckDB",
    )
    load_parser.add_argument(
        "--data-dir",
        type=str,
        help=f"Directory to store downloaded files (default: {DEFAULT_DATA_DIR})",
    )
    load_parser.add_argument(
        "--db-path",
        type=str,
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    load_parser.set_defaults(func=cmd_load)

    # List tables command
    list_parser = subparsers.add_parser(
        "list-tables",
        help="List all tables in the database",
    )
    list_parser.add_argument(
        "--db-path",
        type=str,
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    list_parser.set_defaults(func=cmd_list_tables)

    # Query command
    query_parser = subparsers.add_parser(
        "query",
        help="Execute a SQL query against the database",
    )
    query_parser.add_argument(
        "sql",
        type=str,
        help="SQL query to execute",
    )
    query_parser.add_argument(
        "--db-path",
        type=str,
        help=f"Path to DuckDB database (default: {DEFAULT_DB_PATH})",
    )
    query_parser.set_defaults(func=cmd_query)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
