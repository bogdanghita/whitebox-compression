#!/usr/bin/env python3

import os
import sys
import argparse
import json
from patterns import *
from lib.util import *


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Detect column patterns in CSV file."""
	)

	parser.add_argument('file', metavar='FILE', nargs='?',
		help='CSV file to process. Stdin if none given')
	parser.add_argument('--header-file', dest='header_file', type=str,
		help="CSV file containing the header row (<workbook>/samples/<table>.header-renamed.csv)",
		required=True)
	parser.add_argument('--datatypes-file', dest='datatypes_file', type=str,
		help="CSV file containing the datatypes row (<workbook>/samples/<table>.datatypes.csv)",
		required=True)
	parser.add_argument('--expr-nodes-file', dest='expr_nodes_file', type=str,
		help="Input file containing expression nodes",
		required=True)
	parser.add_argument('--output-dir', dest='output_dir', type=str,
		help="Output dir to write results to",
		required=True)
	parser.add_argument("-F", "--fdelim", dest="fdelim",
		help="Use <fdelim> as delimiter between fields", default="|")
	parser.add_argument("--null", dest="null", type=str,
		help="Interprets <NULL> as NULLs", default="null")

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	with open(args.header_file, 'r') as fd:
		header = list(map(lambda x: x.strip(), fd.readline().split(args.fdelim)))
	with open(args.datatypes_file, 'r') as fd:
		datatypes = list(map(lambda x: x.strip(), fd.readline().split(args.fdelim)))
	if len(header) != len(datatypes):
		return RET_ERR

	columns = []
	for col_id, col_name in enumerate(header):
		columns.append(Column(col_id, col_name, datatypes[col_id]))

	# TODO
	print("[apply_expression] Not implemented")


if __name__ == "__main__":
	main()


"""
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=/scratch/bogdan/master-project/public_bi_benchmark-master_project/benchmark

================================================================================
wb=CommonGovernment
table=CommonGovernment_1
================================================================================
wb=Eixo
table=Eixo_1

================================================================================
input_file=$wbs_dir/$wb/$table.csv
expr_nodes_file=$wbs_dir/$wb/$table.expr_nodes/$table.expr_nodes.json
output_dir=$wbs_dir/$wb/$table.poc_1_out

mkdir -p $output_dir && \
./pattern_detection/apply_expression.py --expr-nodes-file $expr_nodes_file --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv --output-dir $output_dir $input_file

"""
