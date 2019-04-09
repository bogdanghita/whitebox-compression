#!/usr/bin/env python3

import os
import sys
import argparse
import json
import re


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
	res = {}

	# TODO

	return res


def process_compression_log_file(schema, table_name, m_file):
	res = {}

	# TODO

	return res


def data_file_table_match(table_name, f_table):
	return table_name.lower() == f_table.lower()

def data_file_column_match(schema, f_column):
	for col_id, col_data in schema.items():
		col_name_mask = ""
		for c in col_data["col_name"]:
			if c.isalnum():
				col_name_mask += c
			else:
				col_name_mask += "_%x" % (ord(c))
		if col_name_mask.lower() == f_column.lower():
			return col_id
	return None

def get_file_size(file_path):
	# TODO
	return 0

def process_data_files(schema, table_name, m_file):
	res = {}
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

	for col_id, col_data in enumerate(schema):
		col_stats = {}
		if col_id in d_files:
			d_files[col_id]["size_B"] = get_file_size(d_files[col_id]["path"])
			col_stats["data_file"] = d_files[col_id]
		res[col_id] = col_stats

	print(json.dumps(res, indent=2))
	return res


def aggregate_stats(schema, stats):
	res = {}

	# TODO

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
