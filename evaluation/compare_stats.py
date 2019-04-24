#!/usr/bin/env python3

import os
import sys
import argparse
import json


def compare_stats(s_file1, s_file2, expr_nodes_file, apply_expr_stats_file):
	with open(s_file1, 'r') as f1, open(s_file2, 'r') as f2:
		s_data1 = json.load(f1)
		s_data2 = json.load(f2)

	# data_files
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

	print(output)

	# expression nodes: in/out column comparison
	if expr_nodes_file is not None:
		with open(expr_nodes_file, 'r') as f:
			expr_nodes = json.load(f)
		print(json.dumps(expr_nodes, indent=2))
	if apply_expr_stats_file is not None:
		with open(apply_expr_stats_file, 'r') as f:
			apply_expr_stats = json.load(f)
		exception_stats = {}
		for in_col in apply_expr_stats["exceptions"]:
			exception_stats[in_col["col_id"]] = in_col
		print(json.dumps(apply_expr_stats, indent=2))

	if expr_nodes:
		output = "\n*** data_files (column level) ***"

		for expr_n in expr_nodes:
			output += "[{}][score={}]".format(expr_n["p_id"], expr_n["details"]["score"])

			for in_col in expr_n["cols_in"]:
				col_id, col_name = in_col["col_id"], in_col["name"]
				if exception_stats and col_id in exception_stats:
					ex_ratio = exception_stats[col_id]["exception_ratio"]
				if col_id not in s_data1["columns"]:
					print("error: in_col_id={} not found in s_file1={}".format(col_id, s_file1))
					continue
				col_size_B = s_data1["columns"][col_id]["data_files"]["data_file"]["size_B"]
				output += "\ncol_id={}, ex_ratio={}, size_B={}, in_col={}".format(col_id, ex_ratio, col_size_B, in_col)

			expr_n["cols_in"], expr_n["cols_out"]

	print(output)


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
wb=IUBLibrary
table=IUBLibrary_1
================================================================================
wb=Physicians
table=Physicians_1

stats_file_nocompression=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-vectorwise.json
stats_file_default=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json
stats_file_wc=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json
expr_nodes_file=$wbs_dir/$wb/$table.expr_nodes/$table.expr_nodes.json
apply_expr_stats_file=$output_dir/$out_table.stats.json

./evaluation/compare_stats.py $stats_file_nocompression $stats_file_default
./evaluation/compare_stats.py $stats_file_default $stats_file_wc --expr-nodes-file $expr_nodes_file --apply-expr-stats-file $apply_expr_stats_file
"""
