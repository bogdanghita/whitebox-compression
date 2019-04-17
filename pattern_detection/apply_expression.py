#!/usr/bin/env python3

import os
import sys
import argparse
import json
from patterns import *
from lib.util import *


class ExpressionManager(object):
	"""
	NOTE-1: the same column can appear in multiple expression nodes; this is
			because it has multiple patterns; in this case, check each attr
			against all patterns the column appears in; if it matches:
				- exactly one pattern: apply that one
				- more than one pattern: choose one and apply it
				- no pattern: add the attr to the exception column
	"""

	def __init__(self, columns, expr_nodes):
		self.columns = columns
		self.expr_nodes = expr_nodes

	def is_valid_tuple(self, tpl):
		if len(tpl) != len(self.columns):
			return False
		return True

	def apply_expressions(self, tpl):
		if not self.is_valid_tuple(tpl):
			return None
		# TODO
		return ["Not", "implemented"]


def driver_loop(driver, expr_manager, fdelim, fd_out):
	total_tuple_count = 0
	valid_tuple_count = 0

	while True:
		line = driver.nextTuple()
		if line is None:
			break
		total_tuple_count += 1

		tpl = line.split(fdelim)
		tpl_new = expr_manager.apply_expressions(tpl)
		if tpl_new is None:
			continue
		valid_tuple_count += 1

		line_new = fdelim.join(tpl_new)
		fd_out.write(line_new + "\n")

	return (total_tuple_count, valid_tuple_count)


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
	parser.add_argument('--output-file', dest='output_file', type=str,
		help="Output file to write the new data to",
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

	# build columns
	columns = []
	for col_id, col_name in enumerate(header):
		columns.append(Column(col_id, col_name, datatypes[col_id]))

	# build expression nodes
	with open(args.expr_nodes_file, 'r') as f:
		expr_nodes = [ExpressionNode.from_dict(en) for en in json.load(f)]

	# apply expression nodes and generate the new csv file
	expr_manager = ExpressionManager(columns, expr_nodes)
	try:
		if args.file is None:
			fd_in = os.fdopen(os.dup(sys.stdin.fileno()))
		else:
			fd_in = open(args.file, 'r')
		fd_out = open(args.output_file, 'w')

		driver = FileDriver(fd_in, args.fdelim)
		(total_tuple_count, valid_tuple_count) = driver_loop(driver, expr_manager, args.fdelim, fd_out)
	finally:
		fd_in.close()
		try:
			fd_out.close()
		except:
			pass

	print("total_tuple_count={}, valid_tuple_count={}".format(total_tuple_count, valid_tuple_count))


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
wb=Arade
table=Arade_1


================================================================================
input_file=$wbs_dir/$wb/$table.csv
expr_nodes_file=$wbs_dir/$wb/$table.expr_nodes/$table.expr_nodes.json
output_dir=$wbs_dir/$wb/$table.poc_1_out
output_file=$output_dir/$table.csv

mkdir -p $output_dir && \
./pattern_detection/apply_expression.py --expr-nodes-file $expr_nodes_file --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv --output-file $output_file $input_file

"""
