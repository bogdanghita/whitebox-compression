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
	NOTE-2: in_col_data["row_mask"] value convention:
			-1:    exception (cannot be part of any expression node)
			>= 0:  index of the expression node it will be part of
	NOTE-3: it is the operator's responsibility to handle null values and raise
			exception if not supported; for now, they will be added to the
			exceptions column; TODO: handle them better in the future
	"""

	def ROW_MASK_CODE_EXCEPTION = -1

	def __init__(self, in_columns, expr_nodes, null_value):
		self.expr_nodes = expr_nodes
		self.null_value = null_value
		self.in_columns, self.out_columns, self.in_columns_map, self.out_columns_map = [], [], {}, {}
		# populate in_columns & save their indices
		for in_col in in_columns:
			in_col_data = {
				"col_info": in_col,
				"expression_nodes": [],
				"row_mask": [] # see NOTE-2 for values convention
			}
			for expr_n in expression_nodes:
				for c_in in expr_n.cols_in:
					if c_in.col_id == in_col.col_id:
						col_data["expression_nodes"].append(expr_n)
						break
			self.in_columns.append(col_data)
			self.in_columns_map[in_col.col_id] = idx
		# populate out_columns with:
		# 1) unused columns; 2) exception columns for each input column
		for in_col in in_columns:
			# add original column if not present as input in any expression node
			used_columns = [c.col_id for c in expr_n["cols_in"] for expr_n in expression_nodes]
			if in_col.col_id not in used_columns:
				self.out_columns.append(in_col)
				# no need fo exception column if no transformation is made
				continue
			# add exception column
			ex_col = Column(
				col_id = self.get_exception_col_id(in_col.col_id),
				name = str(in_col.name) + "_ex",
				datatype = in_col.datatype
			)
			self.out_columns.append(ex_col)
		# 3) output columns from expression nodes
		for expr_n in expression_nodes:
			self.out_columns.extend(expr_n["cols_out"])
		# save output column indices
		for idx, out_col in enumerate(self.out_columns):
			self.out_columns_map[out_col.col_id] = idx

	@classmethod
	def get_exception_col_id(cls, col_id):
		return str(col_id) + "_ex"

	def is_valid_tuple(self, tpl):
		if len(tpl) != len(self.in_columns):
			return False
		return True

	def apply_expressions(self, in_tpl):
		out_tpl = [self.null_value] * len(self.out_columns)

		if not self.is_valid_tuple(in_tpl):
			return None

		# fill out_tpl in for each expression node
		in_columns_used = set()
		for expr_n_idx, expr_n in enumerate(self.expr_nodes):
			in_attrs = []
			# mark in_col as referenced & get in_attrs
			used = False
			for in_col in expr_n["cols_in"]:
				if in_col.col_id in in_columns_used:
					print("debug: column already used with another expression node")
					used = True
					break
				in_columns_referenced.add(in_col.col_id)
				in_attr = in_tpl[self.in_columns_map[in_col.col_id]]
				in_attrs.append(in_attr)
			if used:
				continue
			# get pattern detector and apply operator
			pd = get_pattern_detector(expr_n["p_id"])
			operator = pd.get_operator(self.null_value)
			try:
				out_attrs = operator(in_attrs, expr_n["cols_in"], expr_n["cols_out"], expr_n["operator_info"])
			except Exception as e:
				# this operator cannot be applied, but other may be; in the worst case, attr is added to the exception column at the end
				continue
			# use this expr_n
			for in_col in expr_n["cols_in"]:
				in_col["row_mask"].append(expr_n_idx)
			# fill in out_tpl
			for out_attr_idx, out_attr in enumerate(out_attrs):
				out_col_idx = self.out_columns_map[expr_n["cols_out"][out_attr_idx].col_id]
				out_tpl[out_col_idx] = out_attr
			# mark in_col as used
			for in_col in expr_n["cols_in"]:
				in_columns_used.add(in_col.col_id)

		# handle unused attrs
		for in_col in self.in_columns:
			if in_col.col_id not in in_columns_used:
				# column not preset as input in any expression node
				if in_col.col_id in self.out_columns_map:
					out_col_idx = self.out_columns_map[in_col.col_id]
					in_col["row_mask"].append(out_col_idx)
				else: # exception
					out_col_idx = self.out_columns_map[self.get_exception_col_id(in_col.col_id)]
					in_col["row_mask"].append(self.ROW_MASK_CODE_EXCEPTION)
				# append attr to out_tpl
				out_tpl[out_col_idx] = in_tpl[self.in_columns_map[in_col.col_id]]

		return out_tpl


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

	# build in_columns
	in_columns = []
	for col_id, col_name in enumerate(header):
		in_columns.append(Column(col_id, col_name, datatypes[col_id]))

	# build expression nodes
	with open(args.expr_nodes_file, 'r') as f:
		expr_nodes = [ExpressionNode.from_dict(en) for en in json.load(f)]

	# apply expression nodes and generate the new csv file
	expr_manager = ExpressionManager(in_columns, expr_nodes)
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
