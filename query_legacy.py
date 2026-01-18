"""Load legacy_data.xlsx into a pandas DataFrame.

Usage:
  python query_legacy.py [--sheet SHEET_NAME]

By default loads all sheets or a specific sheet if provided.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def load_legacy_data(file_path: str | Path = 'legacy_data.xlsx', sheet_name: str | None = None) -> dict[str, pd.DataFrame] | pd.DataFrame:
    """Load Excel file into DataFrame(s).
    
    Args:
        file_path: Path to the Excel file.
        sheet_name: Specific sheet to load, or None to load all sheets.
    
    Returns:
        A single DataFrame if sheet_name is specified, otherwise a dict of DataFrames by sheet name.
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f'{file_path} not found')
    
    return pd.read_excel(file_path, sheet_name=sheet_name)


def main() -> int:
    parser = argparse.ArgumentParser(description='Load legacy_data.xlsx into a DataFrame')
    parser.add_argument('--file', '-f', default='legacy_data.xlsx', help='Path to Excel file')
    parser.add_argument('--sheet', '-s', default=None, help='Specific sheet to load (default: load all)')
    parser.add_argument('--export-json', '-j', default=None, help='Export to JSON file')
    args = parser.parse_args()

    try:
        data = load_legacy_data(args.file, sheet_name=args.sheet)
        
        if isinstance(data, dict):
            print(f'Loaded {len(data)} sheets:')
            for name, df in data.items():
                print(f'  - {name}: {len(df)} rows, {len(df.columns)} columns')
        else:
            print(f'Loaded sheet: {len(data)} rows, {len(data.columns)} columns')
            print(f'Columns: {list(data.columns)}')
        
        if args.export_json:
            if isinstance(data, dict):
                export_data = {k: v.to_dict(orient='records') for k, v in data.items()}
            else:
                export_data = data.to_dict(orient='records')
            
            with open(args.export_json, 'w', encoding='utf8') as f:
                json.dump(export_data, f, indent=2, default=str)
            print(f'Exported to {args.export_json}')
        
        return 0
    except Exception as exc:
        print(f'Error: {exc}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
