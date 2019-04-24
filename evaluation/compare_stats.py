#!/usr/bin/env python3

import os
import sys
import argparse
import json
from lib.util import *


def get_exception_col_name(in_col_name):
	return str(in_col_name) + "_ex"


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


def compare_columns(s_file1, s_file2, s_data1, s_data2, expr_nodes_file, apply_expr_stats_file):
	column_data = {
		s_file1: {c["col_data"]["col_name"]: c for c in s_data1["columns"].values()},
		s_file2: {c["col_data"]["col_name"]: c for c in s_data2["columns"].values()}
	}

	expr_nodes, exception_stats = None, None
	if expr_nodes_file is not None:
		with open(expr_nodes_file, 'r') as f:
			expr_nodes = json.load(f)
		# print(json.dumps(expr_nodes, indent=2))
	if apply_expr_stats_file is not None:
		with open(apply_expr_stats_file, 'r') as f:
			apply_expr_stats = json.load(f)
		exception_stats = {}
		for in_col in apply_expr_stats["exceptions"]:
			exception_stats[in_col["col_id"]] = in_col
		# print(json.dumps(apply_expr_stats, indent=2))

	output = "\n*** data_files (column level) ***"
	
	if expr_nodes:
		# TODO: adapt this code to work recursive expression nodes (i.e. expression trees)
		for expr_n in expr_nodes:
			output += "\n\n[{}][score={:.2f}]".format(expr_n["p_id"], expr_n["details"]["score"])

			# in_cols
			in_size_B = 0
			output += "\n[input_colums]"
			for in_col in expr_n["cols_in"]:
				col_id, col_name = in_col["col_id"], in_col["name"]
				if exception_stats and col_id in exception_stats:
					ex_ratio = exception_stats[col_id]["exception_ratio"]
				if col_name not in column_data[s_file1]:
					print("error: in_col_id={} not found in s_file1={}".format(col_id, s_file1))
					continue
				col_size_B = column_data[s_file1][col_name]["data_files"]["data_file"]["size_B"]
				in_size_B += col_size_B
				output += "\ncol_name={}, ex_ratio={:.2f}, size={}, in_col={}".format(col_name, ex_ratio, sizeof_fmt(col_size_B), in_col)

			# out_cols
			out_size_B = 0
			output += "\n[output_colums]"
			for out_col in expr_n["cols_out"]:
				col_id, col_name = out_col["col_id"], out_col["name"]
				if col_name not in column_data[s_file2]:
					print("error: out_col_id={} not found in s_file2={}".format(col_id, s_file2))
					continue
				col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
				out_size_B += col_size_B
				output += "\ncol_name={}, size={}, out_col={}".format(col_name, sizeof_fmt(col_size_B), out_col)

			# exception columns
			ex_size_B = 0
			output += "\n[exception_colums]"
			for in_col in expr_n["cols_in"]:
				col_name = get_exception_col_name(in_col["name"])
				if col_name not in column_data[s_file2]:
					print("error: col_name={} not found in s_file2={}".format(col_name, s_file2))
					continue
				col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
				ex_size_B += col_size_B
				output += "\ncol_name={}, size={}, out_col={}".format(col_name, sizeof_fmt(col_size_B), out_col)

			# summary
			total_out_size_B = out_size_B + ex_size_B
			compression_ratio = float(in_size_B) / total_out_size_B if total_out_size_B > 0 else float("inf")
			output += "\n[summary]"
			output += "\ncompression_ratio={compression_ratio:.2f}, in_size_B={in_size}, total_out_size_B={total_out_size} (out_size={out_size}, ex_size={ex_size})".format(
					compression_ratio=compression_ratio,
					in_size=sizeof_fmt(in_size_B),
					total_out_size=sizeof_fmt(total_out_size_B),
					out_size=sizeof_fmt(out_size_B),
					ex_size=sizeof_fmt(ex_size_B)
				)

	return output


def compare_stats(s_file1, s_file2, expr_nodes_file, apply_expr_stats_file):
	with open(s_file1, 'r') as f1, open(s_file2, 'r') as f2:
		s_data1 = json.load(f1)
		s_data2 = json.load(f2)

	# data_files
	output_df = compare_data_files(s_file1, s_file2, s_data1, s_data2)
	print(output_df)

	# expression nodes: in/out column comparison
	output_cols = compare_columns(s_file1, s_file2, s_data1, s_data2, expr_nodes_file, apply_expr_stats_file)
	print(output_cols)


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Compare stats"""
	)

	parser.add_argument('baseline_f', type=str,
		help="Path to baseline stats file")
	parser.add_argument('target_f', type=str,
		help="Path to target stats file")
	parser.add_argument('--expr-nodes-file', dest='expr_nodes_file', type=str,
		help="Path to expression nodes file used for transforming baseline into target. If provided, comparison is also done at column level")
	parser.add_argument('--apply-expr-stats-file', dest='apply_expr_stats_file', type=str,
		help="Path to stats file resulted after apply_expressions.py. Only useful if expr-nodes-file is also provided")

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	compare_stats(args.baseline_f, args.target_f, args.expr_nodes_file, args.apply_expr_stats_file)


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
wb=Physicians
table=Physicians_1

out_table="${table}_out"
stats_file_nocompression=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-vectorwise.json
stats_file_default=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json
stats_file_wc=$wbs_dir/$wb/$table.poc_1_out/$out_table.eval-vectorwise.json
expr_nodes_file=$wbs_dir/$wb/$table.expr_nodes/$table.expr_nodes.json
apply_expr_stats_file=$wbs_dir/$wb/$table.poc_1_out/$out_table.stats.json

./evaluation/compare_stats.py $stats_file_nocompression $stats_file_default
./evaluation/compare_stats.py $stats_file_default $stats_file_wc --expr-nodes-file $expr_nodes_file --apply-expr-stats-file $apply_expr_stats_file
"""
