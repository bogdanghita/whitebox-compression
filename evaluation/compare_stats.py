#!/usr/bin/env python3

import os
import sys
import argparse
import json


def compare_stats(s_file1, s_file2):

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


def main():
	if len(sys.argv) != 3:
		usage = """./compare_stats.py <stats-file-1> <stats-file-2>\n
		stats-file example: $table.eval-vectorwise.json"""
		print(usage)
	s_file1, s_file2 = sys.argv[1], sys.argv[2]

	compare_stats(s_file1, s_file2)


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
