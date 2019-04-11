#!/usr/bin/env python3

import os
import sys
import argparse
import json
import re
from lib.data_files_stats import process_data_files
from lib.compression_log_stats import process_compression_log_file
from lib.statdump_stats import process_statdump_file


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
