#!/usr/bin/env python3

import os
import sys
import argparse
import json
from lib.util import *
from pattern_detection.patterns import *
from pattern_detection.lib.expression_tree import *


class ValidationException(Exception):
	def __init__(self, message=None, diff=[]):
		Exception.__init__(self, message)
		self.diff = diff


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Detect column patterns in CSV file."""
	)

	parser.add_argument('file', metavar='FILE', nargs='?',
		help='CSV file to process. Stdin if none given')
	parser.add_argument('--nulls-file', dest='nulls_file', type=str,
		help="Path to null mask file",
		required=True)
	parser.add_argument('--expr-tree-file', dest='expr_tree_file', type=str,
		help="Input file containing expression nodes",
		required=True)
	parser.add_argument('--output-file', dest='output_file', type=str,
		help="Path to output file to write decompressed data to",
		required=True)
	parser.add_argument('--validation-file', dest='validation_file', type=str,
		help="Original uncompressed file to check (de)compression correctness")
	parser.add_argument("-F", "--fdelim", dest="fdelim",
		help="Use <fdelim> as delimiter between fields", default="|")
	parser.add_argument("--null", dest="null", type=str,
		help="Interprets <NULL> as NULLs", default="null")

	return parser.parse_args()


def decompress(in_tpl, null_mask, decompression_tree, topological_order, exception_columns):
	"""
	Input:
		- decompression nodes in topological order
		- (orig_col, ex_col) pairs
		- in tuple

	NOTE-1:
		- for each ex_col, if value is not null, fill in orig_col
		- for each dec_node, if orig_col(s) are not already filled, apply operator_dec(operator_dec_info)
	NOTE-2:
		- find a reliable way to check if an orig_col is already filled:
			- just checking for null might not be enough
			- the evaluation of the dec_node may have resulted in null
			- see if this is problematic and whether you need to separately keep track of which columns were filled
	"""

	print(in_tpl)
	print(null_mask)

	out_tpl = []
	# TODO

	print(out_tpl)
	return out_tpl


def validate(out_tpl, valid_tpl):
	if len(out_tpl) != len(valid_tpl):
		raise ValidationException("Tuple length mismatch",
			diff=dict(out_len=len(out_tpl), valid_len=len(valid_tpl)))

	diff = []
	for idx, (out_attr, valid_attr) in enumerate(zip(out_tpl, valid_tpl)):
		if out_attr != valid_attr:
			diff.append(dict(index=idx, out=out_attr, valid=valid_attr))

	if len(diff) > 0:
		raise ValidationException("Attribute mismatch", diff=diff)


def driver_loop(driver_in, driver_nulls, fdelim, fd_out,
				decompression_tree, topological_order, exception_columns):
	total_tuple_count = 0

	while True:
		in_line = driver.nextTuple()
		if in_line is None:
			break
		total_tuple_count += 1

		in_tpl = in_line.split(fdelim)

		nulls_line = driver_nulls.nextTuple()
		null_mask = nulls_line.split(fdelim)

		out_tpl = decompress(in_tpl, null_mask,
							 decompression_tree, topological_order, exception_columns)

		line_new = fdelim.join(out_tpl)
		fd_out.write(line_new + "\n")

		# debug: print progress
		if total_tuple_count % 100000 == 0:
			print("[progress] total_tuple_count={}M".format(float(total_tuple_count) / 1000000))
		# end-debug


def driver_loop_valid(driver_in, driver_nulls, driver_valid, fdelim, fd_out,
					  decompression_tree, topological_order, exception_columns):
	total_tuple_count = 0

	while True:
		in_line = driver_in.nextTuple()
		valid_line = driver_valid.nextTuple()
		if in_line is None and valid_line is None:
			break
		if not (in_line is not None and valid_line is not None):
			print("error: validation error: number of rows do not match")
			break
		total_tuple_count += 1

		in_tpl = in_line.split(fdelim)
		valid_tpl = valid_line.split(fdelim)

		nulls_line = driver_nulls.nextTuple()
		null_mask = nulls_line.split(fdelim)

		out_tpl = decompress(in_tpl, null_mask,
							 decompression_tree, topological_order, exception_columns)

		try:
			validate(out_tpl, valid_tpl)
		except ValidationException as e:
			print("error:", e)
			print(json.dumps(e.diff, indent=2))

		line_new = fdelim.join(out_tpl)
		fd_out.write(line_new + "\n")

		# debug: print progress
		if total_tuple_count % 100000 == 0:
			print("[progress] total_tuple_count={}M".format(float(total_tuple_count) / 1000000))
		# end-debug


def main():
	args = parse_args()
	print(args)

	# load decompression tree
	decompression_tree = read_expr_tree(args.expr_tree_file)
	if len(decompression_tree.levels) == 0:
		print("debug: empty decompression tree")
		return

	# get ex_col dict:
	exception_columns = {}
	for col_id in decompression_tree.columns.keys():
		ex_col_id = OutputColumnManager.get_exception_col_id(col_id)
		if ex_col_id in decompression_tree.columns:
			exception_columns[col_id] = ex_col_id
	# print(exception_columns)

	# get topological order for evalution
	topological_order = decompression_tree.get_topological_order()
	# print(topological_order)

	# apply decompression tree and generate the decompressed csv file
	try:
		if args.file is None:
			fd_in = os.fdopen(os.dup(sys.stdin.fileno()))
		else:
			fd_in = open(args.file, 'r')
		driver_in = FileDriver(fd_in)
		with open(args.output_file, 'w') as fd_out, open(args.nulls_file, 'r') as fd_nulls:
			driver_nulls = FileDriver(fd_nulls)
			if args.validation_file is None:
				driver_loop(driver_in, driver_nulls, args.fdelim, fd_out,
							decompression_tree, topological_order, exception_columns)
			else:
				with open(args.validation_file, 'r') as fd_valid:
					driver_valid = FileDriver(fd_valid)
					driver_loop_valid(driver_in, driver_nulls, driver_valid, args.fdelim, fd_out,
									  decompression_tree, topological_order, exception_columns)
	finally:
		try:
			fd_in.close()
		except:
			pass


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
wb=Generico
table=Generico_2

================================================================================
expr_tree_file=$wbs_dir/$wb/$table.expr_tree/dec_tree.json
output_file=$wbs_dir/$wb/$table.poc_1_out/$table.decompressed.csv
validation_file=$wbs_dir/$wb/$table.csv
input_file=$wbs_dir/$wb/$table.poc_1_out/${table}_out.csv
nulls_file=$wbs_dir/$wb/$table.poc_1_out/${table}_out.nulls.csv

./decompression/main.py --nulls-file $nulls_file --expr-tree-file $expr_tree_file --output-file $output_file --validation-file $validation_file $input_file
"""
