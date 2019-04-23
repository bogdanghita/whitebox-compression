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
	NOTE-2: p_mask value convention:
			-2:    not used in any expression node
			-1:    exception (cannot be part of any expression node)
			>= 0:  index of the expression node it will be part of
	NOTE-3: it is the operator's responsibility to handle null values and raise
			exception if not supported; for now, they will be added to the
			exceptions column; TODO: handle them better in the future
	"""

	ROW_MASK_CODE_NOT_USED = -2
	ROW_MASK_CODE_EXCEPTION = -1

	def __init__(self, in_columns, expr_nodes, null_value):
		self.null_value = null_value
		self.expr_nodes = []

		# populate expr_nodes
		for expr_n in expr_nodes:
			pd = get_pattern_detector(expr_n.p_id)
			operator = pd.get_operator(expr_n.cols_in, expr_n.cols_out, expr_n.operator_info, self.null_value)
			self.expr_nodes.append({
				"expr_n": expr_n,
				"operator": operator
			})

		self.in_columns, self.out_columns, self.in_columns_map, self.out_columns_map = [], [], {}, {}

		# populate in_columns & save their indices
		for idx, in_col in enumerate(in_columns):
			self.in_columns.append(in_col)
			self.in_columns_map[in_col.col_id] = idx

		# populate out_columns with:
		# 1) unused columns; 2) exception columns for each input column
		for in_col in in_columns:
			# add original column if not present as input in any expression node
			used_columns = [c.col_id for expr_n in expr_nodes for c in expr_n.cols_in]
			if in_col.col_id not in used_columns:
				self.out_columns.append(in_col)
				# no need for exception column if no transformation is made
				continue
			# add exception column
			ex_col = Column(
				col_id = self.get_exception_col_id(in_col.col_id),
				name = str(in_col.name) + "_ex",
				datatype = deepcopy(in_col.datatype)
			)
			ex_col.datatype.nullable = True
			self.out_columns.append(ex_col)
		# 3) output columns from expression nodes
		for expr_n in expr_nodes:
			self.out_columns.extend(expr_n.cols_out)
		# save output column indices
		for idx, out_col in enumerate(self.out_columns):
			self.out_columns_map[out_col.col_id] = idx

		# create stats columns
		self.in_columns_stats = []
		for idx, in_col in enumerate(in_columns):
			in_col_stats = {
				"col_id": in_col.col_id,
				"exception_count": 0
			}
			self.in_columns_stats.append(in_col_stats)

		# # TODO: debug
		# print("***expression_nodes***")
		# for expr_n in expr_nodes:
		# 	print(expr_n.p_id)
		# print("/n***in_columns***")
		# for c in self.in_columns:
		# 	print(c)
		# print("/n***in_columns_map***")
		# for k,c in self.in_columns_map.items():
		# 	print(k,c)
		# print("/n***out_columns***")
		# for c in self.out_columns:
		# 	print(c)
		# print("/n***out_columns_map***")
		# for k,c in self.out_columns_map.items():
		# 	print(k,c)
		# # TODO: end-debug

	def dump_out_schema(self, fd, out_table_name):
		line = "CREATE TABLE \"{}\"(".format(out_table_name)
		fd.write(line + "\n")

		for idx, out_col in enumerate(self.out_columns):
			line = "  \"{}\" {}".format(out_col.name, out_col.datatype.to_sql_str())
			if idx < len(self.out_columns)-1:
				line += ","
			fd.write(line + "\n")

		line = ");"
		fd.write(line + "\n")

	def get_stats(self, valid_tuple_count, total_tuple_count):
		in_col_stats = deepcopy(self.in_columns_stats)
		for in_col_s in in_col_stats:
			in_col_s["exception_ratio"] = float(in_col_s["exception_count"]) / valid_tuple_count if valid_tuple_count > 0 else float("inf")
		valid_tuple_ratio = float(valid_tuple_count) / total_tuple_count if total_tuple_count > 0 else float("inf")
		stats = {
			"total_tuple_count": total_tuple_count,
			"valid_tuple_count": valid_tuple_count,
			"valid_tuple_ratio": valid_tuple_ratio,
			"in_col_stats": in_col_stats
		}
		return stats

	@classmethod
	def get_exception_col_id(cls, col_id):
		return str(col_id) + "_ex"

	def is_valid_tuple(self, tpl):
		if len(tpl) != len(self.in_columns):
			return False
		return True

	def apply_expressions(self, in_tpl):
		out_tpl = [self.null_value] * len(self.out_columns)
		p_mask = [str(self.ROW_MASK_CODE_NOT_USED)] * len(self.in_columns)

		if not self.is_valid_tuple(in_tpl):
			return None

		# fill out_tpl in for each expression node
		in_columns_used = set()
		for expr_n_idx, expr_n in enumerate(self.expr_nodes):
			expr_n, operator = expr_n["expr_n"], expr_n["operator"]
			in_attrs = []
			# mark in_col as referenced & get in_attrs
			used = False
			for in_col in expr_n.cols_in:
				if in_col.col_id in in_columns_used:
					print("debug: column already used with another expression node")
					used = True
					break
				in_attr = in_tpl[self.in_columns_map[in_col.col_id]]
				in_attrs.append(in_attr)
			if used:
				continue
			# apply operator
			try:
				out_attrs = operator(in_attrs)
			except OperatorException as e:
				# this operator cannot be applied, but others may be; in the worst case, attr is added to the exception column at the end
				# print("debug: OperatorException: {}".format(e))
				for in_col in expr_n.cols_in:
					in_col_idx = self.in_columns_map[in_col.col_id]
					self.in_columns_stats[in_col_idx]["exception_count"] += 1
				continue
			# mark in_col as used
			for in_col in expr_n.cols_in:
				in_columns_used.add(in_col.col_id)
			# use this expr_n
			for in_col in expr_n.cols_in:
				in_col_idx = self.in_columns_map[in_col.col_id]
				p_mask[in_col_idx] = str(expr_n_idx)
			# fill in out_tpl
			for out_attr_idx, out_attr in enumerate(out_attrs):
				out_col_idx = self.out_columns_map[expr_n.cols_out[out_attr_idx].col_id]
				out_tpl[out_col_idx] = str(out_attr)

		# handle unused attrs
		for in_col_idx, in_col in enumerate(self.in_columns):
			if in_col.col_id not in in_columns_used:
				# column not preset as input in any expression node
				if in_col.col_id in self.out_columns_map:
					out_col_idx = self.out_columns_map[in_col.col_id]
				else: # exception
					out_col_idx = self.out_columns_map[self.get_exception_col_id(in_col.col_id)]
					p_mask[in_col_idx] = str(self.ROW_MASK_CODE_EXCEPTION)
				# append attr to out_tpl
				out_tpl[out_col_idx] = str(in_tpl[in_col_idx])

		return (out_tpl, p_mask)


def driver_loop(driver, expr_manager, fdelim, fd_out, fd_p_mask):
	total_tuple_count = 0
	valid_tuple_count = 0

	while True:
		line = driver.nextTuple()
		if line is None:
			break
		total_tuple_count += 1

		tpl = line.split(fdelim)
		res = expr_manager.apply_expressions(tpl)
		if res is None:
			continue
		valid_tuple_count += 1

		(tpl_new, p_mask) = res

		line_new = fdelim.join(tpl_new)
		fd_out.write(line_new + "\n")
		line_new = fdelim.join(p_mask)
		fd_p_mask.write(line_new + "\n")

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
	parser.add_argument('--output-dir', dest='output_dir', type=str,
		help="Output dir to put output files in",
		required=True)
	parser.add_argument('--out-table-name', dest='out_table_name', type=str,
		help="Name of the table",
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
		datatypes = list(map(lambda x: DataType.from_sql_str(x.strip()), fd.readline().split(args.fdelim)))
	if len(header) != len(datatypes):
		return RET_ERR

	# build in_columns
	in_columns = []
	for col_id, col_name in enumerate(header):
		in_columns.append(Column(col_id, col_name, datatypes[col_id]))

	# build expression nodes
	with open(args.expr_nodes_file, 'r') as f:
		expr_nodes = [ExpressionNode.from_dict(en) for en in json.load(f)]

	# init expression manager
	expr_manager = ExpressionManager(in_columns, expr_nodes, args.null)

	# generate new schema file with output columns
	schema_file = os.path.join(args.output_dir, "{}.table.sql".format(args.out_table_name))
	with open(schema_file, 'w') as fd_s:
		expr_manager.dump_out_schema(fd_s, args.out_table_name)

	# apply expression nodes and generate the new csv file
	output_file = os.path.join(args.output_dir, "{}.csv".format(args.out_table_name))
	p_mask_file = os.path.join(args.output_dir, "{}.p_mask.csv".format(args.out_table_name))
	try:
		if args.file is None:
			fd_in = os.fdopen(os.dup(sys.stdin.fileno()))
		else:
			fd_in = open(args.file, 'r')
		fd_out = open(output_file, 'w')
		fd_p_mask = open(p_mask_file, 'w')

		driver = FileDriver(fd_in, args.fdelim)
		(total_tuple_count, valid_tuple_count) = driver_loop(driver, expr_manager, args.fdelim, fd_out, fd_p_mask)
	finally:
		fd_in.close()
		try:
			fd_out.close()
			fd_p_mask.close()
		except:
			pass

	# output stats
	stats = expr_manager.get_stats(valid_tuple_count, total_tuple_count)
	stats_file = os.path.join(args.output_dir, "{}.stats.json".format(args.out_table_name))
	with open(stats_file, 'w') as fd_s:
		json.dump(stats, fd_s, indent=2)

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
out_table="${table}_out"

[apply-expression]
mkdir -p $output_dir && \
time ./pattern_detection/apply_expression.py --expr-nodes-file $expr_nodes_file --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv --output-dir $output_dir --out-table-name $out_table $input_file


[load & evaluation]
n_input_file=$output_dir/$out_table.csv
n_schema_file=$output_dir/$out_table.table.sql
db_name=pbib
source ~/.ingVWsh

time ./evaluation/main.sh $db_name $n_input_file $n_schema_file $out_table $output_dir

cat $output_dir/stats-vectorwise/$out_table.statdump.out | less
cat $output_dir/stats-vectorwise/$out_table.compression-log.out | less
cat $output_dir/load-vectorwise/$out_table.data-files.out | less
cat $output_dir/$out_table.eval-vectorwise.json | less

[compare]
stats_file_nocompression=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-vectorwise.json
stats_file_default=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json
stats_file_wc=$output_dir/$out_table.eval-vectorwise.json
./evaluation/compare_stats.py $stats_file_default $stats_file_wc


================================================================================
less $output_dir/$out_table.table.sql
cat $output_dir/$out_table.csv | less -S
awk -F "|" '{ print $2, " ", $57 }' $output_dir/$out_table.csv | less -S
awk -F "|" '{ print $3, " ", $58, $59, $60, $61 }' $output_dir/$out_table.csv | less -S
"""
