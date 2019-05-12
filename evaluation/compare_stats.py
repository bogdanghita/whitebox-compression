#!/usr/bin/env python3

import os
import sys
import argparse
import json
from lib.util import *
from lib_pattern_detection.expression_tree import *


def compare_data_files(s_file1, s_file2, s_data1, s_data2):
	table_data_files = {
		s_file1: s_data1["table"]["data_files"],
		s_file2: s_data2["table"]["data_files"]
	}

	output = "*** data_files (table level) ***"

	output += "\n[stats_file(1)][{}]\n".format(s_file1)
	for k,v in table_data_files[s_file1].items():
		output += "{}: {}\n".format(k, v)

	output += "\n[stats_file(2)][{}]\n".format(s_file2)
	for k,v in table_data_files[s_file2].items():
		output += "{}: {}\n".format(k, v)

	output += "\n[ratio]\n"
	size_B1, size_B2 = table_data_files[s_file1]["size_B"], table_data_files[s_file2]["size_B"]
	size_B_ratio = float(size_B1) / size_B2 if size_B2 != 0 else float("inf")
	output += "size_B(1) / size_B(2) = %.2f" % (size_B_ratio)

	return output


# def compare_columns(s_file1, s_file2, s_data1, s_data2, expr_nodes_file, apply_expr_stats_file):
# 	column_data = {
# 		s_file1: {c["col_data"]["col_name"]: c for c in s_data1["columns"].values()},
# 		s_file2: {c["col_data"]["col_name"]: c for c in s_data2["columns"].values()}
# 	}

# 	expr_nodes, exception_stats = None, None
# 	if expr_nodes_file is not None:
# 		with open(expr_nodes_file, 'r') as f:
# 			expr_nodes = json.load(f)
# 		# print(json.dumps(expr_nodes, indent=2))
# 	if apply_expr_stats_file is not None:
# 		with open(apply_expr_stats_file, 'r') as f:
# 			apply_expr_stats = json.load(f)
# 		exception_stats = {}
# 		for in_col in apply_expr_stats["exceptions"]:
# 			exception_stats[in_col["col_id"]] = in_col
# 		# print(json.dumps(apply_expr_stats, indent=2))

# 	output = "\n*** data_files (column level) ***"

# 	if expr_nodes:
# 		# TODO: adapt this code to work on recursive expression nodes (i.e. expression trees)
# 		# TODO: in the current implementation: exception columns are counted for every expression node, even though they are shared between all expression nodes they are input of; TODO: find a smarter way to evaluate the performance in this case (e.g. take the exception ratio of each individual expr_n and multiply it with the size of the exception column; then also add the remaining size of the column which corresponds to the nulls)
# 		for expr_n in expr_nodes:
# 			output += "\n\n[{}][coverage={:.2f}]".format(expr_n["p_id"], expr_n["details"]["coverage"])

# 			# in_cols
# 			in_size_B = 0
# 			output += "\n[input_colums]"
# 			for in_col in expr_n["cols_in"]:
# 				col_id, col_name = in_col["col_id"], in_col["name"]
# 				if exception_stats and col_id in exception_stats:
# 					ex_ratio = exception_stats[col_id]["exception_ratio"]
# 				if col_name not in column_data[s_file1]:
# 					raise Exception("error: in_col_name={} not found in s_file1={}".format(col_name, s_file1))
# 				col_size_B = column_data[s_file1][col_name]["data_files"]["data_file"]["size_B"]
# 				in_size_B += col_size_B
# 				output += "\ncol_id={}, col_name={}, ex_ratio={:.2f}, size={}, in_col={}".format(in_col["col_id"], col_name, ex_ratio, sizeof_fmt(col_size_B), in_col)

# 			# out_cols
# 			out_size_B = 0
# 			output += "\n[output_colums]"
# 			for out_col in expr_n["cols_out"]:
# 				col_id, col_name = out_col["col_id"], out_col["name"]
# 				if col_name not in column_data[s_file2]:
# 					raise Exception("out_col_name={} not found in s_file2={}".format(col_name, s_file2))
# 				col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
# 				out_size_B += col_size_B
# 				output += "\ncol_id={}, col_name={}, size={}, out_col={}".format(in_col["col_id"], col_name, sizeof_fmt(col_size_B), out_col)

# 			# exception columns
# 			# ex_size_B = 0
# 			# output += "\n[exception_colums]"
# 			# for in_col in expr_n["cols_in"]:
# 			# 	col_name = ExceptionColumnManager.get_exception_col_name(in_col["name"])
# 			# 	if col_name not in column_data[s_file2]:
# 			# 		raise Exception("col_name={} not found in s_file2={}".format(col_name, s_file2))
# 			# 	col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
# 			# 	ex_size_B += col_size_B
# 			# 	output += "\ncol_id={}, col_name={}, size={}, out_col={}".format(in_col["col_id"], col_name, sizeof_fmt(col_size_B), out_col)
# 			ex_size_B = 0
# 			output += "\n[exception_colums]"
# 			for ex_col in expr_n["cols_ex"]:
# 				col_id, col_name = ex_col["col_id"], ex_col["name"]
# 				if col_name not in column_data[s_file2]:
# 					raise Exception("ex_col_name={} not found in s_file2={}".format(col_name, s_file2))
# 				col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
# 				ex_size_B += col_size_B
# 				output += "\ncol_id={}, col_name={}, size={}, ex_col={}".format(in_col["col_id"], col_name, sizeof_fmt(col_size_B), ex_col)

# 			# summary
# 			total_out_size_B = out_size_B + ex_size_B
# 			compression_ratio = float(in_size_B) / total_out_size_B if total_out_size_B > 0 else float("inf")
# 			output += "\n[summary]"
# 			output += "\np_id={p_id}, compression_ratio={compression_ratio:.2f}, in_size_B={in_size}, total_out_size_B={total_out_size} (out_size={out_size}, ex_size={ex_size})".format(
# 					p_id=expr_n["p_id"],
# 					compression_ratio=compression_ratio,
# 					in_size=sizeof_fmt(in_size_B),
# 					total_out_size=sizeof_fmt(total_out_size_B),
# 					out_size=sizeof_fmt(out_size_B),
# 					ex_size=sizeof_fmt(ex_size_B)
# 				)
# 			# compression_ratio = float(in_size_B) / out_size_B if out_size_B > 0 else float("inf")
# 			# output += "\n[summary]"
# 			# output += "\np_id={p_id}, compression_ratio={compression_ratio:.2f}, in_size_B={in_size}, out_size={out_size}".format(
# 			# 		p_id=expr_n["p_id"],
# 			# 		compression_ratio=compression_ratio,
# 			# 		in_size=sizeof_fmt(in_size_B),
# 			# 		out_size=sizeof_fmt(out_size_B)
# 			# 	)

# 	return output


def compare_columns(s_file1, s_file2, s_data1, s_data2, expr_tree_file, apply_expr_stats_file):
	column_data = {
		s_file1: {c["col_data"]["col_name"]: c for c in s_data1["columns"].values()},
		s_file2: {c["col_data"]["col_name"]: c for c in s_data2["columns"].values()}
	}

	expr_tree, exception_stats = None, None
	if expr_tree_file is not None:
		expr_tree = read_expr_tree(expr_tree_file)
	if apply_expr_stats_file is not None:
		with open(apply_expr_stats_file, 'r') as f:
			apply_expr_stats = json.load(f)
		exception_stats = {}
		# for in_col in apply_expr_stats["exceptions"]:
		# 	exception_stats[in_col["col_id"]] = in_col
		# print(json.dumps(apply_expr_stats, indent=2))

	# print(expr_tree.to_dict())
	ccs = expr_tree.get_connected_components()
	for cc in ccs:
		print(cc)
	sys.exit(1)

	output = "\n*** data_files (column level) ***"

	if expr_tree:
		# TODO: adapt this code to work on recursive expression nodes (i.e. expression trees)
		# TODO: in the current implementation: exception columns are counted for every expression node, even though they are shared between all expression nodes they are input of; TODO: find a smarter way to evaluate the performance in this case (e.g. take the exception ratio of each individual expr_n and multiply it with the size of the exception column; then also add the remaining size of the column which corresponds to the nulls)

		# for expr_n in expr_nodes:
			# output += "\n\n[{}][coverage={:.2f}]".format(expr_n["p_id"], expr_n["details"]["coverage"])

		for cc_expr_tree in expr_tree.get_connected_components():
			in_columns = [cc_expr_tree.get_column(col_id) for col_id in cc_expr_tree.get_in_columns()]
			out_columns = [cc_expr_tree.get_column(col_id) for col_id in cc_expr_tree.get_out_columns()]

			# in_cols
			in_size_B = 0
			output += "\n[input_colums]"
			for col_item in in_columns:
				in_col = col_item["col_info"]
				col_id, col_name = in_col["col_id"], in_col["name"]
				# if exception_stats and col_id in exception_stats:
				# 	ex_ratio = exception_stats[col_id]["exception_ratio"]
				if col_name not in column_data[s_file1]:
					raise Exception("error: in_col_name={} not found in s_file1={}".format(col_name, s_file1))
				col_size_B = column_data[s_file1][col_name]["data_files"]["data_file"]["size_B"]
				in_size_B += col_size_B
				# output += "\ncol_id={}, col_name={}, ex_ratio={:.2f}, size={}, in_col={}".format(in_col["col_id"], col_name, ex_ratio, sizeof_fmt(col_size_B), in_col)
				output += "\ncol_id={}, col_name={}, size={}, in_col={}".format(in_col["col_id"], col_name, sizeof_fmt(col_size_B), in_col)

			# out_cols
			out_size_B = 0
			output += "\n[output_colums]"
			for col_item in out_columns:
				out_col = col_item["col_info"]
				# handle expcetion columns separately
				if ExceptionColumnManager.is_exception_col(out_col):
					continue
				col_id, col_name = out_col["col_id"], out_col["name"]
				if col_name not in column_data[s_file2]:
					raise Exception("out_col_name={} not found in s_file2={}".format(col_name, s_file2))
				col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
				out_size_B += col_size_B
				output += "\ncol_id={}, col_name={}, size={}, out_col={}".format(in_col["col_id"], col_name, sizeof_fmt(col_size_B), out_col)

			# exception columns
			# ex_size_B = 0
			# output += "\n[exception_colums]"
			# for in_col in expr_n["cols_in"]:
			# 	col_name = ExceptionColumnManager.get_exception_col_name(in_col["name"])
			# 	if col_name not in column_data[s_file2]:
			# 		raise Exception("col_name={} not found in s_file2={}".format(col_name, s_file2))
			# 	col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
			# 	ex_size_B += col_size_B
			# 	output += "\ncol_id={}, col_name={}, size={}, out_col={}".format(in_col["col_id"], col_name, sizeof_fmt(col_size_B), out_col)
			ex_size_B = 0
			output += "\n[exception_colums]"
			for col_item in out_columns:
				ex_col = col_item["col_info"]
				if not ExceptionColumnManager.is_exception_col(out_col):
					continue
				col_id, col_name = ex_col["col_id"], ex_col["name"]
				if col_name not in column_data[s_file2]:
					raise Exception("ex_col_name={} not found in s_file2={}".format(col_name, s_file2))
				col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
				ex_size_B += col_size_B
				output += "\ncol_id={}, col_name={}, size={}, ex_col={}".format(in_col["col_id"], col_name, sizeof_fmt(col_size_B), ex_col)

			# summary
			total_out_size_B = out_size_B + ex_size_B
			compression_ratio = float(in_size_B) / total_out_size_B if total_out_size_B > 0 else float("inf")
			output += "\n[summary]"
			output += "\np_id={p_id}, compression_ratio={compression_ratio:.2f}, in_size_B={in_size}, total_out_size_B={total_out_size} (out_size={out_size}, ex_size={ex_size})".format(
					p_id=expr_n["p_id"],
					compression_ratio=compression_ratio,
					in_size=sizeof_fmt(in_size_B),
					total_out_size=sizeof_fmt(total_out_size_B),
					out_size=sizeof_fmt(out_size_B),
					ex_size=sizeof_fmt(ex_size_B)
				)
			# compression_ratio = float(in_size_B) / out_size_B if out_size_B > 0 else float("inf")
			# output += "\n[summary]"
			# output += "\np_id={p_id}, compression_ratio={compression_ratio:.2f}, in_size_B={in_size}, out_size={out_size}".format(
			# 		p_id=expr_n["p_id"],
			# 		compression_ratio=compression_ratio,
			# 		in_size=sizeof_fmt(in_size_B),
			# 		out_size=sizeof_fmt(out_size_B)
			# 	)

	return output


def compare_stats(s_file1, s_file2, expr_tree_file, apply_expr_stats_file):
	with open(s_file1, 'r') as f1, open(s_file2, 'r') as f2:
		s_data1 = json.load(f1)
		s_data2 = json.load(f2)

	# data_files
	try:
		output_df = compare_data_files(s_file1, s_file2, s_data1, s_data2)
		print(output_df)
	except Exception as e:
		print("error: unable to perform: compare_data_files; e={}".format(e))

	# expression nodes: in/out column comparison
	try:
		output_cols = compare_columns(s_file1, s_file2, s_data1, s_data2, expr_tree_file, apply_expr_stats_file)
		print(output_cols)
	except Exception as e:
		print("error: unable to perform: compare_columns; e={}".format(e))


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Compare stats"""
	)

	parser.add_argument('baseline_f', type=str,
		help="Path to baseline stats file")
	parser.add_argument('target_f', type=str,
		help="Path to target stats file")
	parser.add_argument('--expr-tree-file', dest='expr_tree_file', type=str,
		help="Path to expression nodes file used for transforming baseline into target. If provided, comparison is also done at column level")
	parser.add_argument('--apply-expr-stats-file', dest='apply_expr_stats_file', type=str,
		help="Path to stats file resulted after apply_expressions.py. Only useful if expr-tree-file is also provided")

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	compare_stats(args.baseline_f, args.target_f, args.expr_tree_file, args.apply_expr_stats_file)


if __name__ == "__main__":
	main()


"""
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=/scratch/bogdan/master-project/public_bi_benchmark-master_project/benchmark

================================================================================
wb=Eixo
table=Eixo_1
================================================================================
wb=Arade
table=Arade_1
================================================================================
wb=CommonGovernment
table=CommonGovernment_1
================================================================================
wb=IUBLibrary
table=IUBLibrary_1
================================================================================
wb=Generico
table=Generico_1

out_table="${table}_out"
stats_file_nocompression=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-vectorwise.json
stats_file_default=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json
stats_file_wc=$wbs_dir/$wb/$table.poc_1_out/$out_table.eval-vectorwise.json
expr_tree_file=$wbs_dir/$wb/$table.expr_tree/expr_tree.json
apply_expr_stats_file=$wbs_dir/$wb/$table.poc_1_out/$out_table.stats.json

./evaluation/compare_stats.py $stats_file_nocompression $stats_file_default
./evaluation/compare_stats.py $stats_file_default $stats_file_wc --expr-tree-file $expr_tree_file --apply-expr-stats-file $apply_expr_stats_file


================================================================================
# [run-all]
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=../public_bi_benchmark-master_project/benchmark
testset_dir=testsets/testset_unique_schema

for wb in $testset_dir/*; do \
  for table in $(cat $wb); do \
    wb="$(basename $wb)"; \
    echo $wb $table; \
\
    out_table="${table}_out"
    stats_file_nocompression=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-vectorwise.json; \
    stats_file_default=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json; \
    stats_file_wc=$wbs_dir/$wb/$table.poc_1_out/$out_table.eval-vectorwise.json; \
    expr_tree_file=$wbs_dir/$wb/$table.expr_tree/expr_tree.json; \
    apply_expr_stats_file=$wbs_dir/$wb/$table.poc_1_out/$out_table.stats.json; \
    output_dir=$wbs_dir/$wb/$table.poc_1_out/compare_stats; \
\
    if test -f "$stats_file_wc"; then \
      mkdir -p $output_dir; \
      ./evaluation/compare_stats.py $stats_file_nocompression $stats_file_default &> $output_dir/$table.compare_stats.nocompression-default.out; \
      ./evaluation/compare_stats.py $stats_file_default $stats_file_wc --expr-tree-file $expr_tree_file --apply-expr-stats-file $apply_expr_stats_file &> $output_dir/$table.compare_stats.default-wc.out; \
    fi; \
\
  done; \
done

# get results on local machine
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
dst_dir=./evaluation/output/poc_1/output_1
mkdir -p $dst_dir

scp bogdan@bricks14:$wbs_dir/*/*.poc_1_out/compare_stats/*.compare_stats.*.out $dst_dir

"""
