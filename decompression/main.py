#!/usr/bin/env python3

import os
import sys
import argparse
import json
import traceback
from lib.util import *
from pattern_detection.patterns import *
from pattern_detection.lib.expression_tree import *


class ValidationException(Exception):
	def __init__(self, message=None, diff=[]):
		Exception.__init__(self, message)
		self.diff = diff


class DecompressionContext(object):
	def __init__(self, decompression_tree, input_header, output_header, null_value):
		self.decompression_tree = decompression_tree
		self.null_value = null_value

		def get_col_by_name(col_name):
			for col_id, col_item in decompression_tree.columns.items():
				if col_item["col_info"].name == col_name:
					return col_item["col_info"]
			raise Exception("Invalid col_name: {}".format(col_name))

		# in_columns, out_columns
		self.in_columns = decompression_tree.get_in_columns()
		self.out_columns = decompression_tree.get_out_columns()

		# # unused out columns dict
		# self.unused_out_columns = [col_id for col_id in decompression_tree.get_unused_columns()
		# 							if not OutputColumnManager.is_exception_col(decompression_tree.get_column(col_id)["col_info"])]
		# print("unused_out_columns: {}".format(self.unused_out_columns))

		# exception columns dict
		self.exception_columns = dict()
		for col_id in decompression_tree.columns.keys():
			ex_col_id = OutputColumnManager.get_exception_col_id(col_id)
			if ex_col_id in decompression_tree.columns:
				self.exception_columns[ex_col_id] = col_id
		print("exception_columns: {}".format(self.exception_columns))

		# column positions
		self.in_column_positions, self.out_column_positions = dict(), dict()
		for idx, col_name in enumerate(input_header):
			col_id = get_col_by_name(col_name).col_id
			self.in_column_positions[col_id] = idx
		for idx, col_name in enumerate(output_header):
			col_id = get_col_by_name(col_name).col_id
			self.out_column_positions[col_id] = idx
		print("in_column_positions: {}".format(self.in_column_positions))
		print("out_column_positions: {}".format(self.out_column_positions))

		# topological order for evalution
		# self.topological_order = decompression_tree.get_topological_order()
		# print("topological_order: {}".format(self.topological_order))

		# decompression nodes in topological order
		self.decompression_nodes = []
		for node_id in decompression_tree.get_topological_order():
			expr_n = decompression_tree.get_node(node_id)
			pd = get_pattern_detector(expr_n.p_id)
			operator = pd.get_operator_dec(expr_n.cols_in, expr_n.cols_out, expr_n.operator_info, self.null_value)
			self.decompression_nodes.append({
				"node_id": node_id,
				"expr_n": expr_n,
				"operator": operator
			})

		# sys.exit(1)


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Detect column patterns in CSV file."""
	)

	parser.add_argument('file', metavar='FILE', nargs='?',
		help='CSV file to process. Stdin if none given')
	parser.add_argument('--in-header-file', dest='in_header_file', type=str,
		help="Path to input header file (header of the compressed table)",
		required=True)
	parser.add_argument('--out-header-file', dest='out_header_file', type=str,
		help="Path to output header file (header of the original table)",
		required=True)
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


def decompress(in_tpl, null_mask, context):
	# print(in_tpl)
	# print(null_mask)

	# debug: debug-values
	# global values
	# end-debug: debug-values
	values = dict()

	# input columns (used & unused)
	for col_id in context.in_columns:
		# debug
		# if col_id == "19":
		# 	print("input columns")
		# # end-debug
		values[col_id] = in_tpl[context.in_column_positions[col_id]]
		# print("[unused columns] col_id={}".format(col_id))

	# exceptions
	for ex_col_id, col_id in context.exception_columns.items():
		attr = in_tpl[context.in_column_positions[ex_col_id]]
		if attr != context.null_value:
			# debug
			# if col_id == "19":
			# 	print("exceptions")
			# end-debug
			values[col_id] = attr
			# print("[exceptions] col_id={}".format(col_id))

	# null values
	for col_id in context.out_columns:
		out_col_pos = context.out_column_positions[col_id]
		if null_mask[out_col_pos]:
			# debug
			# if col_id == "19":
			# 	print("null_values")
			# 	print(out_col_pos, null_mask[out_col_pos])
			# end-debug
			values[col_id] = context.null_value
			# print("[null values] col_id={}".format(col_id))

	# apply operators in topological order
	for expr_n in context.decompression_nodes:
		node_id, expr_n, operator = expr_n["node_id"], expr_n["expr_n"], expr_n["operator"]

		# fill in in_attrs
		in_attrs = []
		abort = False
		for in_col in expr_n.cols_in:
			try:
				in_attr = values[in_col.col_id]
			except KeyError as e:
				# expr_n that was supposed to generate values[in_col.col_id] was not used in the compression
				abort = True
			in_attrs.append(in_attr)

		# debug
		# if "19" in {c.col_id for c in expr_n.cols_out}:
		# 	print("expr_n:", node_id, expr_n.p_id)
		# 	print(expr_n.cols_in)
		# 	print(expr_n.cols_in_consumed)
		# 	print(in_attrs)
		# 	print(expr_n.operator_info)
		# 	print(abort)
		# end-debug

		if abort:
			continue

		# apply operator
		try:
			out_attrs = operator(in_attrs)
		except OperatorException as e:
			# debug
			# if "19" in {c.col_id for c in expr_n.cols_out}:
			# 	print("OperatorException:", e)
			# end-debug
			# expr_n was not used in the compression
			continue

		# fill in values with out_attrs
		for out_col_idx, out_col in enumerate(expr_n.cols_out):
			""" don't fill value if already filled; reasons:
				1) exception
				2) value was null
				3) compression took a different path
			"""
			if out_col.col_id in values:
				# debug
				# if "19" in {c.col_id for c in expr_n.cols_out}:
				# 	print("out_col.col_id in values:", out_col.col_id, values[out_col.col_id])
				# end-debug
				continue
			values[out_col.col_id] = out_attrs[out_col_idx]

	# fill in out_tpl
	out_tpl = [context.null_value] * len(context.out_columns)
	for col_id in context.out_columns:
		if col_id not in values:
			# debug: debug-values
			# print(json.dumps(values, indent=2))
			# print("total_tuple_count={}".format(total_tuple_count))
			# end-debug: debug-values
			raise Exception("error: value not filled: col_id={}".format(col_id))
		out_col_pos = context.out_column_positions[col_id]
		out_tpl[out_col_pos] = values[col_id]

	# print(out_tpl, "\n")
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
		# debug: debug-values
		# print("total_tuple_count={}".format(total_tuple_count))
		# end-debug: debug-values
		raise ValidationException("Attribute mismatch", diff=diff)


def driver_loop(driver_in, driver_nulls, fdelim, fd_out,
				decompression_context):
	global total_tuple_count
	total_tuple_count = 0

	while True:
		in_line = driver.nextTuple()
		if in_line is None:
			break
		total_tuple_count += 1

		in_tpl = in_line.split(fdelim)

		nulls_line = driver_nulls.nextTuple()
		null_mask = [True if v == "1" else False for v in nulls_line.split(fdelim)]

		out_tpl = decompress(in_tpl, null_mask, decompression_context)

		line_new = fdelim.join(out_tpl)
		fd_out.write(line_new + "\n")

		# debug: print progress
		if total_tuple_count % 100000 == 0:
			print("[progress] total_tuple_count={}M".format(float(total_tuple_count) / 1000000))
		# end-debug


def driver_loop_valid(driver_in, driver_nulls, driver_valid, fdelim, fd_out,
					  decompression_context):
	global total_tuple_count
	total_tuple_count = 0

	while True:
		in_line = driver_in.nextTuple()
		valid_line = driver_valid.nextTuple()
		if in_line is None and valid_line is None:
			break
		if not (in_line is not None and valid_line is not None):
			print("error: validation error: number of rows do not match; total_tuple_count={}".format(total_tuple_count))
			break
		total_tuple_count += 1

		in_tpl = in_line.split(fdelim)
		valid_tpl = valid_line.split(fdelim)

		nulls_line = driver_nulls.nextTuple()
		null_mask = [True if v == "1" else False for v in nulls_line.split(fdelim)]

		out_tpl = decompress(in_tpl, null_mask, decompression_context)

		try:
			validate(out_tpl, valid_tpl)
		except ValidationException as e:
			print("error:", e)
			# debug: debug-values
			# if total_tuple_count == 1:
			# 	print(json.dumps(values, indent=2))
			print(json.dumps(e.diff, indent=2))
			sys.exit(1)
			# end-debug: debug-values

		line_new = fdelim.join(out_tpl)
		fd_out.write(line_new + "\n")

		# debug: print progress
		if total_tuple_count % 100000 == 0:
			print("[progress] total_tuple_count={}M".format(float(total_tuple_count) / 1000000))
		# end-debug


def main():
	args = parse_args()
	print(args)

	# load headers
	with open(args.in_header_file, 'r') as fd:
		input_header = list(map(lambda x: x.strip(), fd.readline().split(args.fdelim)))
	with open(args.out_header_file, 'r') as fd:
		output_header = list(map(lambda x: x.strip(), fd.readline().split(args.fdelim)))

	# load decompression tree
	decompression_tree = read_expr_tree(args.expr_tree_file)
	if len(decompression_tree.levels) == 0:
		print("debug: empty decompression tree")
		return

	# build decompression context
	decompression_context = DecompressionContext(decompression_tree, input_header, output_header, args.null)

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
							decompression_context)
			else:
				with open(args.validation_file, 'r') as fd_valid:
					driver_valid = FileDriver(fd_valid)
					driver_loop_valid(driver_in, driver_nulls, driver_valid, args.fdelim, fd_out,
									  decompression_context)
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
in_header_file=$wbs_dir/$wb/$table.poc_1_out/${table}_out.header.csv
out_header_file=$repo_wbs_dir/$wb/samples/$table.header-renamed.csv
expr_tree_file=$wbs_dir/$wb/$table.expr_tree/dec_tree.json
output_file=$wbs_dir/$wb/$table.poc_1_out/$table.decompressed.csv
validation_file=$wbs_dir/$wb/$table.csv
input_file=$wbs_dir/$wb/$table.poc_1_out/${table}_out.csv
nulls_file=$wbs_dir/$wb/$table.poc_1_out/${table}_out.nulls.csv

time ./decompression/main.py --in-header-file $in_header_file --nulls-file $nulls_file --expr-tree-file $expr_tree_file \
--output-file $output_file --out-header-file $out_header_file \
--validation-file $validation_file \
$input_file
"""
