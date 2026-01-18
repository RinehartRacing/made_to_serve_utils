("""Load legacy Excel sheets and return them as plain Python lists of row dicts.

This module calls `load_legacy_data` from `query_legacy.py` and exposes
`get_all_sheets_records()` which returns a dict: {sheet_name: [row_dict, ...]}.

It also provides a small CLI to write the combined JSON to a file.
""")

from __future__ import annotations

from unicodedata import name
import uuid
import json
import datetime
from pathlib import Path
import sys
from typing import Dict, List, Any, Set

import numpy as np
import pandas as pd
import re
import argparse
from query_legacy import load_legacy_data



def _format_phone(phone: str | None) -> str | None:
	"""Normalize phone to (XXX) XXX-XXXX when possible, otherwise return original string or None."""
	if phone is None:
		return None
	# handle numeric types and Excel float representations (e.g. 15125898513.0)
	if isinstance(phone, (int, float, np.integer, np.floating)):
		# for floats that are integer-valued, drop decimal
		try:
			if float(phone).is_integer():
				s = str(int(phone))
			else:
				s = str(phone)
		except Exception:
			s = str(phone)
	else:
		s = str(phone).strip()
	# drop any decimal fraction remaining (e.g. '15125898513.0' -> '15125898513')
	if '.' in s:
		s = s.split('.')[0]
	digits = ''.join(c for c in s if c.isdigit())
	# drop leading country code '1' if present
	if len(digits) == 11 and digits.startswith('1'):
		digits = digits[1:]
	if len(digits) == 10:
		return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
	return s


def get_all_sheets_records(file: str | Path = 'legacy_data.xlsx') -> Dict[str, List[List[Any]]]:
	"""Return all sheets as a dict mapping sheet name -> list-of-rows.

	Each sheet is represented as a list of rows; the first row is the header
	(column names as strings) followed by data rows as value lists.

	Args:
		file: path to the Excel workbook.

	Returns:
		Dict mapping sheet name to list of records (each record is a dict).
	"""
	data = load_legacy_data(file, sheet_name=None)
	# `load_legacy_data` returns a dict[str, DataFrame]
	result: Dict[str, List[Dict[str, Any]]] = {}
	def _convert_df(df: pd.DataFrame) -> List[List[Any]]:
		# Ensure column names are strings
		df = df.copy()
		df.columns = [str(c) for c in df.columns]
		# Replace NaN/NaT with None
		df = df.where(pd.notnull(df), None)

		def _convert_value(v):
			if v is None:
				return None
			if isinstance(v, (pd.Timestamp, datetime.datetime, datetime.date)):
				return v.isoformat()
			if isinstance(v, (np.integer,)):
				return int(v)
			if isinstance(v, (np.floating,)):
				return float(v)
			if isinstance(v, (np.bool_,)):
				return bool(v)
			if isinstance(v, (np.ndarray, list, tuple)):
				try:
					return list(v)
				except Exception:
					return str(v)
			return v

		cols = [str(c) for c in df.columns]
		records: List[List[Any]] = []
		# first row is header
		records.append(cols)
		for row in df.to_dict(orient='records'):
			rec = [_convert_value(row.get(c)) for c in cols]
			records.append(rec)
		return records

	for name, df in data.items():
		result[name] = _convert_df(df)
	return result

def get_all_names(combined: Dict[str, List[List[Any]]]) -> List[str]:
	names = set()
	name_column = {"2024 Handouts": 0, "2024 Meal Prep": 0, "2025 Saturday Handout": 0, "2025 Sunday Handouts": 0, "2025 Meal Prep": 0, "Volunteer Info": 0}
	for sheet, rows in combined.items():
		column = name_column.get(sheet, 0)
		for row in rows[1:]:
			if len(row) > column:
				name = row[column]
				if type(name) == str and len(name.split()) >= 2:
					# Make all characters in name lower case except first character of each word
					name = ' '.join(word.capitalize() for word in name.split())
					names.add(name)

	names_list = list(names)
	names_list.sort()
	return names_list


def get_new_value(csv_file: str | Path, column: str) -> List[Any]:
	"""Retrieve all values from a given column in a CSV file.

	Args:
		csv_file: Path to the CSV file.
		column: Column name to extract values from.

	Returns:
		List of all non-None values in that column.
	"""
	df = pd.read_csv(csv_file)
	if column not in df.columns:
		raise ValueError(f'Column "{column}" not found in {csv_file}. Available columns: {list(df.columns)}')
	values = df[column].dropna().tolist()
	return values


def do_merging(combined: Dict[str, List[List[Any]]]) -> None:
	names_legacy = get_all_names(combined)
	names_new = get_new_value("data/users.csv", "name")
	only_in_new = set(names_new) - set(names_legacy)
	only_in_legacy = set(names_legacy) - set(names_new)
	all_names = set(names_legacy).union(set(names_new))
	generate_migrated_users(only_in_legacy, combined)
	generate_migrated_opportunities(combined)


def get_email_from_name(name: str, combined: Dict[str, List[List[Any]]]) -> str | None:
	sheet = "Volunteer Info"
	if sheet in combined:
		rows = combined[sheet]
		if len(rows) > 0:
			headers = rows[0]
			if 'Email' in headers:
				email_col = headers.index('Email')
				for row in rows[1:]:
					if len(row) > 0 and row[0].lower() == name.lower() and len(row) > email_col:
						return row[email_col]
	return None

def get_phone_from_name(name: str, combined: Dict[str, List[List[Any]]]) -> str | None:
	sheet = "Volunteer Info"
	if sheet in combined:
		rows = combined[sheet]
		if len(rows) > 0:
			headers = rows[0]
			if 'Phone' in headers:
				phone_col = headers.index('Phone')
				for row in rows[1:]:
					if len(row) > 0 and row[0].lower() == name.lower() and len(row) > phone_col:
						phone = row[phone_col]
						if phone:
							# delegate formatting to centralized helper
							return _format_phone(phone)
						return None
	return None

def get_legacy_opportunities(combined: Dict[str, List[List[Any]]]) -> Dict[str, List[str]]:
	"""Retrieve legacy opportunities handouts as a dict mapping date -> list of opportunity names.

	Args:
		combined: Combined legacy data sheets."""
	opportunities: Dict[str, List[str]] = {}
	ignored_columns = ["Volunteers:", "Total Hours:"]
	sheets_of_interest = ["2024 Handouts", "2024 Meal Prep","2025 Saturday Handout", "2025 Sunday Handouts", "2025 Meal Prep", "2026 Saturday Handout", "2026 Sunday Handouts", "2026 Meal Prep"]
	seventh = "seventh"
	riverside = "riverside"
	menchaca = "menchaca"
	meal_prep = "Meal Prep"
	first_day_of_saturday = datetime.datetime.strptime("4/5/2025", "%m/%d/%Y")
	first_day_of_riverside = datetime.datetime.strptime("10/18/2025", "%m/%d/%Y")
	missed_day = datetime.datetime.strptime("3/3/2024", "%m/%d/%Y")
	for sheet in sheets_of_interest:
		# print(sheet)
		row = combined[sheet][0]
		for col in row:
			# Convert date string in the format "2024-05-05 00:00:00" to "5/5/2024"
			if re.match(r'^\d{4}-\d{2}-\d{2} 00:00:00$', str(col)):
				col = datetime.datetime.strptime(col, '%Y-%m-%d %H:%M:%S').strftime('%#m/%#d/%Y')
			# Only process columns that look likes dates in the format 1/10/2026
			if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', str(col)):
				# If col is a Saturday
				col_date = datetime.datetime.strptime(col, '%m/%d/%Y')
				# If date hasn't happened yet, then skip
				if col_date > datetime.datetime.now():
					continue
				# If date is missed day then skip
				if col_date.date() == missed_day.date():
					continue
				date_str = col_date.strftime('%#m/%#d/%Y')
				if col_date.weekday() == 5:  # Saturday
					if col_date < first_day_of_saturday:
						opportunities[date_str] = [meal_prep]
					elif col_date < first_day_of_riverside:
						opportunities[date_str] = [seventh, meal_prep]
					else:
						opportunities[date_str] = [riverside, seventh, meal_prep]
				elif col_date.weekday() == 6:  # Sunday
					if col_date < first_day_of_saturday:
						opportunities[date_str] = [seventh]
					else:
						opportunities[date_str] = [menchaca]
				else:
					sys.exit(f"Unexpected weekday for date column {col} in sheet {sheet}")
			elif col in ignored_columns:
				continue
			elif re.match(r'^\d{1,2}/\d{1,2}/\d{2,4}\-\d{1,2}\/\d{1,2}\/\d{2,4}$', str(col)):
				# If it's a date range like 1/10/2026-1/17/2026, extract each Sunday in this range
				start_str, end_str = str(col).split('-')
				# Handle both 2-digit and 4-digit years
				start_date = datetime.datetime.strptime(start_str, '%m/%d/%y' if len(start_str.split('/')[-1]) == 2 else '%m/%d/%Y').date()
				end_date = datetime.datetime.strptime(end_str, '%m/%d/%y' if len(end_str.split('/')[-1]) == 2 else '%m/%d/%Y').date()
				current_date = start_date

				while current_date <= end_date:
					# If date is missed day then skip
					if current_date == missed_day.date():
						current_date += datetime.timedelta(days=1)
						continue
					if current_date.weekday() == 6:  # Sunday
						date_str = current_date.strftime('%#m/%#d/%Y')
						if date_str not in opportunities:
							opportunities[date_str] = [seventh]
					current_date += datetime.timedelta(days=1)
			else:
				print(f"Not a date string: \"{col}\" in sheet {sheet}", file=sys.stderr)
				print(type(col))
				sys.exit()
	# Sort opportunities by date and print
	# sorted_dates = sorted(opportunities.keys(), key=lambda d: datetime.datetime.strptime(d, '%m/%d/%Y'))
	# for date in sorted_dates:
	# 	print(f"{date}: {opportunities[date]}")
	return opportunities

def generate_migrated_opportunities(combined: Dict[str, List[List[Any]]], output_csv: str | Path = "data/opportunities_migrated.csv") -> None:
	"""Generate a new opportunities CSV with normalized phone numbers.

	Args:
		combined: Combined legacy data sheets.
		output_csv: Path to write the migrated opportunities CSV.
	"""
	# Load existing opportunities
	df = pd.read_csv("data/opportunities.csv")
	# opportunities will be a dictionary that maps date to a list of opportunity names
	legacy_opportunities = get_legacy_opportunities(combined)
	# Loop through df
	for index, row in df.iterrows():
		print(row)
		date = row["datetime"]
		# Handle timezone offset that may be +00 instead of +0000
		if isinstance(date, str) and date.endswith('+00'):
			date = date + '00'  # Convert +00 to +0000
		date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S%z').strftime('%#m/%#d/%Y')
		if date in legacy_opportunities:
			continue
		else:
			# If date is in the future, skip
			date_obj = datetime.datetime.strptime(date, '%m/%d/%Y')
			if date_obj > datetime.datetime.now():
				continue
			print("Not in opportunities")
			print(date)
			sys.exit()
	for legacy_opportunity in legacy_opportunities.items():
		date, opportunity_names = legacy_opportunity
		for opportunity_name in opportunity_names:
			# Check if this date and opportunity_name already exists in df
			matches = df[(df['datetime'].str.contains(date)) & (df['name'] == opportunity_name)]
			if len(matches) == 0:
				# Create new row
				new_row = {
					'id': str(uuid.uuid4()),
					"image_url": None,
					'title': opportunity_name,
					'datetime': f"{datetime.datetime.strptime(date, '%m/%d/%Y').strftime('%Y-%m-%d')} 09:00:00-0500",
					"location": "location coming soon",
					'description': "description coming soon",
					'spot_left': 0,
					"created_at": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S%z'),
					"Ended": "t",
					"start_time": "start time coming soon",
					"end_time": "end time coming soon",
					"redemption_code": None,
					"hours_approved": "f",
					"location_link": None
				}
				# Append new row to df
				df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
	# Write to CSV with UTF-8 encoding
	df.to_csv(output_csv, index=False, encoding='utf-8')
	print(f'Wrote {output_csv} ({len(df)} total rows)')


def generate_migrated_users(only_in_legacy: Set[str], combined: Dict[str, List[List[Any]]], users_csv: str | Path = "data/users.csv", output_csv: str | Path = "data/users_migrated.csv") -> None:
	"""Generate a new users CSV with rows from users.csv plus new rows for names only in legacy.

	Args:
		only_in_legacy: Set of names that are in legacy but not in users.csv
		users_csv: Path to the original users.csv
		output_csv: Path to write the migrated users.csv
	"""
	# Load existing users
	df = pd.read_csv(users_csv)
	# Normalize phone numbers in existing rows using centralized helper
	if 'phone_number' in df.columns:
		df['phone_number'] = df['phone_number'].apply(lambda v: _format_phone(v) if pd.notna(v) else v)
	
	# Create new rows for legacy-only names
	new_rows = []
	for name in sorted(only_in_legacy):
		row = {col: None for col in df.columns}
		row['id'] = str(uuid.uuid4())
		row['name'] = name
		row['admin'] = "f"
		row['email_verified'] = "f"
		row['email'] = get_email_from_name(name, combined) or None
		# get_phone_from_name now returns a formatted phone or None
		row["phone_number"] = get_phone_from_name(name, combined)
		row["subscribe_newsletter"] = "f"
		new_rows.append(row)
	
	# Append new rows to dataframe
	new_df = pd.DataFrame(new_rows)
	migrated_df = pd.concat([df, new_df], ignore_index=True)
	
	# Write to CSV with UTF-8 encoding
	migrated_df.to_csv(output_csv, index=False, encoding='utf-8')
	print(f'Wrote {output_csv} ({len(migrated_df)} total rows, {len(new_rows)} new entries)')

def main() -> int:
	parser = argparse.ArgumentParser(description='Export all legacy sheets to a combined JSON file')
	parser.add_argument('--file', '-f', default='legacy_data.xlsx', help='Path to Excel file')
	parser.add_argument('--out', '-o', default='legacy_all.json', help='Output JSON file')
	args = parser.parse_args()

	try:
		combined = get_all_sheets_records(args.file)
		do_merging(combined)
		with open(args.out, 'w', encoding='utf8') as fh:
			json.dump(combined, fh, indent=2, ensure_ascii=False)
		print(f'Wrote {args.out} ({len(combined)} sheets)')
		return 0
	except Exception as exc:
		print('Error:', exc)
		return 1


if __name__ == '__main__':
	raise SystemExit(main())

