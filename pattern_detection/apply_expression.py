#!/usr/bin/env python3

import os
import sys
import argparse
import json
from lib.expression_tree import *
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
	NOTE-2: it is the operator's responsibility to handle null values and raise
			exception if not supported; for now, they will be added to the
			exceptions column; TODO: handle them better in the future
	"""

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
		# 1) unused columns
		for in_col in in_columns:
			# add original column if not present as input in any expression node
			used_columns = [c.col_id for expr_n in expr_nodes for c in expr_n.cols_in]
			if in_col.col_id not in used_columns:
				self.out_columns.append(in_col)
				continue
		# [2) output, 3) exception, 4) unconsumed input] columns from expression nodes
		for expr_n in expr_nodes:
			# output columns
			self.out_columns.extend(expr_n.cols_out)
			# exception columns
			for ex_col in expr_n.cols_ex:
				# NOTE: multiple expr_n can have the same ex_col; add it only once
				if ex_col.col_id not in [c.col_id for c in self.out_columns]:
					self.out_columns.append(ex_col)
			# unconsumed input columns
			for in_col in expr_n.cols_in:
				if in_col.col_id not in {c.col_id for c in expr_n.cols_in_consumed}:
					# NOTE: in_col may have been added already by other expr_n; add it only once
					if in_col.col_id not in [c.col_id for c in self.out_columns]:
						self.out_columns.append(in_col)

		# save output & exception column indices
		for idx, out_col in enumerate(self.out_columns):
			self.out_columns_map[out_col.col_id] = idx

		# create stats columns
		# self.in_columns_stats = []
		# for idx, in_col in enumerate(in_columns):
		# 	in_col_stats = {
		# 		"col_id": in_col.col_id,
		# 		"exception_count": 0
		# 	}
		# 	self.in_columns_stats.append(in_col_stats)
		self.out_columns_stats = []
		for idx, out_col in enumerate(self.out_columns):
			out_col_stats = {
				"col_id": out_col.col_id,
				"null_count": 0
			}
			self.out_columns_stats.append(out_col_stats)

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

	def get_out_columns(self):
		return self.out_columns

	def dump_out_header(self, fd, fdelim):
		line = fdelim.join([col.name for col in self.get_out_columns()])
		fd.write(line + "\n")

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
		# # exception stats
		# in_columns_stats = deepcopy(self.in_columns_stats)
		# ex_col_stats = []
		# for in_col_s in in_columns_stats:
		# 	in_col_s["exception_ratio"] = float(in_col_s["exception_count"]) / valid_tuple_count if valid_tuple_count > 0 else float("inf")
		# 	ex_col_id = OutputColumnManager.get_exception_col_id(in_col_s["col_id"])
		# 	# NOTE: this check is necessary because not all input columns have an exception column
		# 	if ex_col_id in self.out_columns_map:
		# 		ex_col_stats.append(in_col_s)
		# null stats
		out_columns_stats = deepcopy(self.out_columns_stats)
		for out_col_s in out_columns_stats:
			out_col_s["null_ratio"] = float(out_col_s["null_count"]) / valid_tuple_count if valid_tuple_count > 0 else float("inf")
		# other stats
		valid_tuple_ratio = float(valid_tuple_count) / total_tuple_count if total_tuple_count > 0 else float("inf")
		stats = {
			"total_tuple_count": total_tuple_count,
			"valid_tuple_count": valid_tuple_count,
			"valid_tuple_ratio": valid_tuple_ratio,
			"out_columns": out_columns_stats
		}
		return stats

	def is_valid_tuple(self, tpl):
		if len(tpl) != len(self.in_columns):
			return False
		return True

	def apply_expressions(self, in_tpl):
		out_tpl = [self.null_value] * len(self.out_columns)
		null_mask = ["1" if attr == self.null_value else "0" for attr in in_tpl]

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
					# print("debug: column already used with another expression node")
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
				# for in_col in expr_n.cols_in:
				# 	in_col_idx = self.in_columns_map[in_col.col_id]
				# 	self.in_columns_stats[in_col_idx]["exception_count"] += 1
				continue
			# mark in_col as used
			for in_col in expr_n.cols_in:
				in_columns_used.add(in_col.col_id)
			# use this expr_n
			for in_col in expr_n.cols_in:
				in_col_idx = self.in_columns_map[in_col.col_id]
			# fill in out_tpl
			for out_attr_idx, out_attr in enumerate(out_attrs):
				out_col_idx = self.out_columns_map[expr_n.cols_out[out_attr_idx].col_id]
				out_tpl[out_col_idx] = str(out_attr)

		# handle unused attrs
		for in_col_idx, in_col in enumerate(self.in_columns):
			# if column not preset as input in any expression node
			if in_col.col_id not in in_columns_used:
				# attr is null and no expression node handled it
				if in_tpl[in_col_idx] == self.null_value:
					# nothing to be done; out_tpl[out_col_idx] is already null
					continue
				# in_col is an output column
				# NOTE: this also catches unconsumed input columns
				if in_col.col_id in self.out_columns_map:
					out_col_idx = self.out_columns_map[in_col.col_id]
				else: # exception
					out_col_idx = self.out_columns_map[OutputColumnManager.get_exception_col_id(in_col.col_id)]
				# add attr to out_tpl
				out_tpl[out_col_idx] = str(in_tpl[in_col_idx])

		# count nulls for stats
		for idx, attr in enumerate(out_tpl):
			if attr == self.null_value:
				self.out_columns_stats[idx]["null_count"] += 1

		return (out_tpl, null_mask)


def apply_expression_manager_list(tpl, expr_manager_list):
	# print("\n[in_tpl]", len(tpl), tpl)

	# apply all expression managers one after the other
	for expr_manager in expr_manager_list:
	# for idx, expr_manager in enumerate(expr_manager_list):
		res = expr_manager.apply_expressions(tpl)
		if res is None:
			return None
		tpl, null_mask = res

		# print("level: ", idx)
		# print([col.col_id for col in expr_manager.get_out_columns()])
		# print(len(tpl), tpl)
		# print(len(null_mask), null_mask)
		#
		# for idx, col in enumerate(expr_manager.get_out_columns()):
		# 	print(null_mask[idx], tpl[idx], col.col_id)

	# print("[out_tpl]", len(tpl), tpl)
	# print("[null_mask]", len(null_mask), null_mask)
	# sys.exit(1)

	return res


def driver_loop(driver, expr_manager_list, fdelim, fd_out, fd_null_mask):
	total_tuple_count = 0
	valid_tuple_count = 0

	while True:
		line = driver.nextTuple()
		if line is None:
			break
		total_tuple_count += 1

		in_tpl = line.split(fdelim)

		res = apply_expression_manager_list(in_tpl, expr_manager_list)
		if res is None:
			continue
		(out_tpl, null_mask) = res
		valid_tuple_count += 1

		line_new = fdelim.join(out_tpl)
		fd_out.write(line_new + "\n")
		line_new = fdelim.join(null_mask)
		fd_null_mask.write(line_new + "\n")

		# debug: print progress
		if total_tuple_count % 100000 == 0:
			print("[progress] total_tuple_count={}M, valid_tuple_count={}M".format(
				float(total_tuple_count) / 1000000,
				float(valid_tuple_count) / 1000000))
		# end-debug

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
	parser.add_argument('--expr-tree-file', dest='expr_tree_file', type=str,
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

	# build columns
	columns = []
	for idx, col_name in enumerate(header):
		col_id = str(idx)
		columns.append(Column(col_id, col_name, datatypes[idx]))

	# load expression tree
	expression_tree = read_expr_tree(args.expr_tree_file)
	if len(expression_tree.levels) == 0:
		raise Exception("Empty expression tree")

	# debug
	# connected_components = expression_tree.get_connected_components()
	# print("[connected_components] len={}".format(len(connected_components)))
	# for cc_expr_tree in connected_components:
	# 	print(cc_expr_tree.levels)
	# end-debug

	# init expression managers
	expr_manager_list = []
	in_columns = columns
	for idx, level in enumerate(expression_tree.levels):
		expr_nodes = [expression_tree.get_node(node_id) for node_id in level]
		expr_manager = ExpressionManager(in_columns, expr_nodes, args.null)
		expr_manager_list.append(expr_manager)
		# out_columns becomes in_columns for the next level
		in_columns = expr_manager.get_out_columns()

	# generate header and schema files with output columns
	out_header_file = os.path.join(args.output_dir, "{}.header.csv".format(args.out_table_name))
	with open(out_header_file, 'w') as fd_h:
		expr_manager_list[-1].dump_out_header(fd_h, args.fdelim)
	out_schema_file = os.path.join(args.output_dir, "{}.table.sql".format(args.out_table_name))
	with open(out_schema_file, 'w') as fd_s:
		expr_manager_list[-1].dump_out_schema(fd_s, args.out_table_name)

	# apply expression tree and generate the new csv file
	output_file = os.path.join(args.output_dir, "{}.csv".format(args.out_table_name))
	null_mask_file = os.path.join(args.output_dir, "{}.nulls.csv".format(args.out_table_name))
	try:
		if args.file is None:
			fd_in = os.fdopen(os.dup(sys.stdin.fileno()))
		else:
			fd_in = open(args.file, 'r')
		driver = FileDriver(fd_in)
		with open(output_file, 'w') as fd_out, open(null_mask_file, 'w') as fd_null_mask:
			(total_tuple_count, valid_tuple_count) = driver_loop(driver, expr_manager_list, args.fdelim, fd_out, fd_null_mask)
	finally:
		try:
			fd_in.close()
		except:
			pass

	# output stats
	# TODO: these stats are not relevant in the current form; update or discard them
	stats = expr_manager_list[-1].get_stats(valid_tuple_count, total_tuple_count)
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
wb=CMSprovider
table=CMSprovider_1
================================================================================
wb=Generico
table=Generico_2


================================================================================
input_file=$wbs_dir/$wb/$table.csv
expr_tree_file=$wbs_dir/$wb/$table.expr_tree/c_tree.json
output_dir=$wbs_dir/$wb/$table.poc_1_out
out_table="${table}_out"

n_input_file=$output_dir/$out_table.csv
n_schema_file=$output_dir/$out_table.table.sql
wv_n_schema_file=$output_dir/$out_table.table-vectorwise.sql
db_name=pbib
source ~/.ingVWsh

stats_file_nocompression=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-vectorwise.json
stats_file_default=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json
stats_file_wc=$wbs_dir/$wb/$table.poc_1_out/$out_table.eval-vectorwise.json
apply_expr_stats_file=$wbs_dir/$wb/$table.poc_1_out/$out_table.stats.json
summary_out_file=$output_dir/$table.summary.json


# [apply-expression]
mkdir -p $output_dir && \
time ./pattern_detection/apply_expression.py --expr-tree-file $expr_tree_file --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv --output-dir $output_dir --out-table-name $out_table $input_file

cat $output_dir/$out_table.stats.json | less

# [load & evaluation]
./util/VectorWiseify-schema.sh $n_schema_file $wv_n_schema_file > /dev/null
time ./evaluation/main.sh $db_name $n_input_file $wv_n_schema_file $out_table $output_dir

cat $output_dir/stats-vectorwise/$out_table.statdump.out | less
cat $output_dir/stats-vectorwise/$out_table.compression-log.out | less
cat $output_dir/load-vectorwise/$out_table.data-files.out | less
cat $output_dir/$out_table.eval-vectorwise.json | less

# [compare]
# ./evaluation/compare_stats.py $stats_file_nocompression $stats_file_default
./evaluation/compare_stats.py $stats_file_default $stats_file_wc --expr-tree-file $expr_tree_file --apply-expr-stats-file $apply_expr_stats_file --summary-out-file $summary_out_file


================================================================================
less $output_dir/$out_table.table.sql
cat $output_dir/$out_table.csv | less -S
awk -F "|" '{ print $2, " ", $57 }' $output_dir/$out_table.csv | less -S
awk -F "|" '{ print $20, " ", $82 }' $output_dir/$out_table.csv | less -S
awk -F "|" '{ print $48, " ", $90 }' $output_dir/$out_table.csv | less -S
awk -F "|" '{ print $3, " ", $58, $59, $60, $61 }' $output_dir/$out_table.csv | less -S
"""
