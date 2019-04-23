#!/usr/bin/env python3

import os
import sys
import argparse
import json


def compare_stats(s_file1, s_file2, expr_nodes_file):
	# TODO: if expr_nodes_file is not None: also do column-wise comparison

	with open(s_file1, 'r') as f1, open(s_file2, 'r') as f2:
		s_data1 = json.load(f1)
		s_data2 = json.load(f2)

	# data_files
	table_data_files = {
		s_file1: s_data1["table"]["data_files"],
		s_file2: s_data2["table"]["data_files"]
	}
	output = "*** data_files ***"
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

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	compare_stats(args.baseline_f, args.target_f, args.expr_nodes_file)


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

stats_file1=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-vectorwise.json
stats_file2=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json

./evaluation/compare_stats.py $stats_file1 $stats_file2
"""
