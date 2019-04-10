#!/usr/bin/env python3

import os
import sys
import argparse
import json
import re
from lib.util import sizeof_fmt


RET_ERR = 15


"""
NOTE: interested in: compression ratio and size in bytes
TODO: extract stats from the *.statdump.* and *.compression-log.* files
TODO: get size of each column by looking at the data files (listed in *.data-files.*)
"""


def parse_schema_file(schema_file):
	schema = {}
	regex_col = re.compile(r'^"(.*?)" (.*?),?$')

	with open(schema_file, 'r') as f:
		# ignore create table line
		f.readline()
		cols = list(map(lambda c: c.strip(), f.readlines()[:-1]))

	for col_id, c in enumerate(cols):
		m = regex_col.match(c)
		if not m:
			raise Exception("Unable to parse schema file")
		col_name, datatype = m.group(1), m.group(2)
		schema[col_id] = {
			"col_id": col_id,
			"col_name": col_name,
			"datatype": datatype
		}

	return schema


def process_statdump_file(schema, table_name, m_file):
	res = {
		"columns": {},
		"table": {}
	}

	# TODO

	return res


def process_compression_log_file(schema, table_name, m_file):
	res = {
		"columns": {},
		"table": {}
	}

	# TODO

	return res


def data_file_table_match(table_name, f_table):
	return table_name.lower() == f_table.lower()


# def data_file_column_match(schema, f_column):
# 	# NOTE: does not work for some columns
#
# 	additional_accepted_chars = ['_']
#
# 	for col_id, col_data in schema.items():
# 		col_name_mask = ""
# 		for c in col_data["col_name"]:
# 			if (ord(c) < 128 and c.isalnum()) or c in additional_accepted_chars:
# 				col_name_mask += c
# 			else:
# 				col_name_mask += "_%x" % (ord(c))
#
# 		# TODO: debug
# 		if col_id == 5 and "data_20pr_C3_A9_2Dmatricula" in f_column:
# 			print("f_column:\n{}\ncol_data[\"col_name\"]:\n{}\ncol_name_mask:\n{}\n".format(f_column, col_data["col_name"], col_name_mask))
# 		# TODO: end-debug
#
# 		if col_name_mask.lower() == f_column.lower():
# 			return col_id
#
# 	return None


def data_file_column_match(schema, f_column):
	# NOTE: does not work for truncated column names
	# You need something like this:
	# https://stackoverflow.com/questions/10032788/partial-regex-matching-in-python-3-2?rq=1

	def char_type(c):
		if ord(c) < 128 and c.isalnum():
			return 1
		else:
			return 0
	regex_any = "(.*?)"

	for col_id, col_data in schema.items():
		col_name = col_data["col_name"]
		regex_col_name = []

		idx = 0
		# -1 so that will trigger a change at the beginning
		current_char_type = -1
		while idx < len(col_name):
			# new regex group
			c = col_name[idx]
			current_char_type = char_type(c)
			if char_type(c) == 1:
				regex_col_name.append("{}".format(c))
			else:
				regex_col_name.append(regex_any)
			idx += 1
			# go until char type change
			while idx < len(col_name):
				c = col_name[idx]
				# char type change
				if char_type(c) != current_char_type:
					break
				# fill token with char
				if char_type(c) == 1:
					regex_col_name[-1] += c
				# regex_any already added
				else:
					pass
				idx += 1

		# match with f_column
		regex_col_name = re.compile("^" + "".join(regex_col_name), re.IGNORECASE)
		m = regex_col_name.search(f_column)
		if m:
			return col_id

	return None


# def _data_file_tokens_match(tokens, target, placeholder):
# 	# TODO: debug
# 	print(target, tokens)
# 	# TODO: end-debug
#
# 	def findall(p, s):
# 		'''Yields all the positions of
# 		the pattern p in the string s.'''
# 		i = s.find(p)
# 		while i != -1:
# 			yield i
# 			i = s.find(p, i+1)
#
# 	if len(target) == 0:
# 		return True
# 	if len(tokens) == 0:
# 		return True
# 	t = tokens[0]
# 	if t == placeholder:
# 		return _data_file_tokens_match(tokens[1:], target, placeholder)
#
# 	for idx in findall(t, target):
# 		m = _data_file_tokens_match(tokens[1:], target[idx+len(t):], placeholder)
# 		if m:
# 			return True
# 	if t.startswith(target):
# 		return True
#
# 	return False
#
# def _data_file_column_match(col_name, f_column):
# 	# NOTE: simple regex approach does not work for truncated column names
#
# 	# TODO: debug
# 	# if f_column != "data_20pr_C3_A9_2Dmatricula".lower() or "-Matricula".lower() not in col_name:
# 	# 	return None
# 	# TODO: end-debug
#
# 	def char_type(c):
# 		if ord(c) < 128 and c.isalnum():
# 			return 1
# 		else:
# 			return 0
# 	placeholder = "(.*?)"
#
# 	tokens = []
#
# 	idx = 0
# 	# -1 so that will trigger a change at the beginning
# 	current_char_type = -1
# 	while idx < len(col_name):
# 		# new regex group
# 		c = col_name[idx]
# 		current_char_type = char_type(c)
# 		if char_type(c) == 1:
# 			tokens.append("{}".format(c))
# 		else:
# 			tokens.append(placeholder)
# 		idx += 1
# 		# go until char type change
# 		while idx < len(col_name):
# 			c = col_name[idx]
# 			# char type change
# 			if char_type(c) != current_char_type:
# 				break
# 			# fill token with char
# 			if char_type(c) == 1:
# 				tokens[-1] += c
# 			# placeholder already added
# 			else:
# 				pass
# 			idx += 1
#
# 	# match with f_column
# 	m = _data_file_tokens_match(tokens, f_column, placeholder)
# 	if m:
# 		print("[success] f_column:\n{}\ntokens:\n{}\n".format(f_column, tokens))
# 	return m
#
# def data_file_column_match(schema, f_column):
# 	matches = []
# 	for col_id, col_data in schema.items():
# 		m = _data_file_column_match(col_data["col_name"].lower(), f_column.lower())
# 		if m:
# 			matches.append(col_id)
#
# 	if len(matches) > 1:
# 		print("debug: multiple column matches: f_column={}, matches={}".format(f_column, matches))
# 		print([schema[m]["col_name"] for m in matches])
# 	if len(matches) == 1:
# 		return matches[0]
# 	return None


def process_data_files(schema, table_name, m_file):
	res = {
		"columns": {},
		"table": {}
	}
	regex_basename = re.compile(r'^.*?S(.*?)__(.*)_.*?$')

	d_files = {}
	with open(m_file, 'r') as f:
		for df in f:
			df = df.strip()
			basename = os.path.basename(df)
			# parse basename
			m = regex_basename.match(basename)
			if not m:
				print("error: Invalid file format: {}".format(df))
				continue
			f_table = m.group(1)
			f_column = m.group(2)
			# table filter
			if not data_file_table_match(table_name, f_table):
				print("debug: table mismatch for file: {}".format(df))
				continue
			# check column match
			col_id = data_file_column_match(schema, f_column)
			if col_id is None:
				print("debug: column mismatch for file: {}".format(df))
				continue
			d_files[col_id] = {
				"path": df,
				"basename": basename,
				"f_table": f_table,
				"f_column": f_column
			}

	res["table"]["size_B"] = 0
	for col_id, col_data in schema.items():
		col_stats = {}
		if col_id in d_files:
			# column size
			d_files[col_id]["size_B"] = os.path.getsize(d_files[col_id]["path"])
			d_files[col_id]["size_human_readable"] = sizeof_fmt(d_files[col_id]["size_B"])
			col_stats["data_file"] = d_files[col_id]
			# table size_B
			res["table"]["size_B"] += d_files[col_id]["size_B"]
		res["columns"][col_id] = col_stats

	# table size_human_readable
	res["table"]["size_human_readable"] = sizeof_fmt(res["table"]["size_B"])

	return res


def aggregate_stats(schema, stats):
	res = {}

	res_columns = {col_id: {"col_data": col_data} for col_id, col_data in schema.items()}
	res_agg = {}

	for s, s_data in stats.items():
		res_agg[s] = s_data["table"]
		for col_id, col_stats_data in s_data["columns"].items():
			if col_id not in res_columns:
				print("debug: unknown col_id: {}".format(col_id))
				continue
			res_columns[col_id][s] = col_stats_data

	res = {
		"columns": res_columns,
		"table": res_agg
	}

	return res


def output_agg_stats(agg_stats, table_name, output_dir):
	out_f = os.path.join(output_dir, "{}.eval-vectorwise.json".format(table_name))
	with open(out_f, 'w') as f:
		json.dump(agg_stats, f, indent=2)


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Detect column patterns in CSV file."""
	)

	parser.add_argument('--schema-file', dest='schema_file', type=str,
		help="SQL file containing table schema", required=True)
	parser.add_argument('--table-name', dest='table_name', type=str,
		help="Name of the table. Must be the same with the one in the schema-file", required=True)
	parser.add_argument('--output-dir', dest='output_dir', type=str,
		help="Folder containing the load & stats output to process. Further output will also be added to this folder", required=True)

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	schema = parse_schema_file(args.schema_file)

	statdump_stats = process_statdump_file(schema, args.table_name,
						"{}/stats-vectorwise/{}.statdump.out".format(args.output_dir, args.table_name))
	compression_log_stats = process_compression_log_file(schema, args.table_name,
						"{}/stats-vectorwise/{}.compression-log.out".format(args.output_dir, args.table_name))
	data_files_stats = process_data_files(schema, args.table_name,
						"{}/load-vectorwise/{}.data-files.out".format(args.output_dir, args.table_name))

	stats = {
		"statdump": statdump_stats,
		"compression_log": compression_log_stats,
		"data_files": data_files_stats
	}

	agg_stats = aggregate_stats(schema, stats)

	output_agg_stats(agg_stats, args.table_name, args.output_dir)


if __name__ == "__main__":
	main()
