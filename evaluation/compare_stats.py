#!/usr/bin/env python3

import os
import sys
import argparse
import json
from lib.util import *
from pattern_detection.patterns import *
from pattern_detection.lib.expression_tree import *


def get_metadata_size(expr_tree):
	metadata_size_B = 0
	for node_id, expr_n in expr_tree.nodes.items():
		size_B = get_pattern_detector(expr_n.p_id).get_metadata_size(expr_n.operator_info)
		metadata_size_B += size_B
	return metadata_size_B


def compare_data_files(s_file1, s_file2, s_data1, s_data2, expr_tree_file=None, table_name="<table_name>"):
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

	metadata_size_B = None
	if expr_tree_file is not None:
		expr_tree = read_expr_tree(expr_tree_file)
		metadata_size_B = get_metadata_size(expr_tree)
		output += "metadata_size_B: {}\nmetadata_size_human_readable: {}\n".format(metadata_size_B, sizeof_fmt(metadata_size_B))
	else:
		output += "metadata: no information available\n"

	output += "\n[ratio]\n"
	size_B1, size_B2 = table_data_files[s_file1]["size_B"], table_data_files[s_file2]["size_B"]
	if metadata_size_B is not None:
		size_B2 += metadata_size_B
	size_B_ratio = float(size_B1) / size_B2 if size_B2 != 0 else float("inf")
	output += "[%s] size(1)=%s, size(2)=%s, table_compression_ratio=%.2f" % (table_name, sizeof_fmt(size_B1), sizeof_fmt(size_B2), size_B_ratio)

	return output


def compare_ccs(s_file1, s_file2, s_data1, s_data2, expr_tree_file, apply_expr_stats_file, table_name="<table_name>"):
	column_data = {
		s_file1: {c["col_data"]["col_name"]: c for c in s_data1["columns"].values()},
		s_file2: {c["col_data"]["col_name"]: c for c in s_data2["columns"].values()}
	}

	expr_tree, out_columns_stats = None, None
	if expr_tree_file is not None:
		expr_tree = read_expr_tree(expr_tree_file)
	if apply_expr_stats_file is not None:
		with open(apply_expr_stats_file, 'r') as f:
			apply_expr_stats = json.load(f)
		out_columns_stats = {}
		for out_col in apply_expr_stats["out_columns"]:
			out_columns_stats[out_col["col_id"]] = out_col
		# print(json.dumps(apply_expr_stats, indent=2))

	ccs = expr_tree.get_connected_components()
	# for cc in ccs:
	# 	print("levels={}, in_columns={}".format(cc.levels, cc.get_in_columns()))

	agg_output = "\n*** data files (aggregated; used columns) ***"
	ccs_output = "\n*** data_files (connected components) ***"

	if expr_tree:
		agg_in_size_B, agg_total_out_size_B = 0, 0

		for cc_expr_tree in expr_tree.get_connected_components():
			in_columns = [cc_expr_tree.get_column(col_id) for col_id in cc_expr_tree.get_in_columns()]
			out_columns = [cc_expr_tree.get_column(col_id) for col_id in cc_expr_tree.get_out_columns()]

			# in_cols
			in_size_B = 0
			ccs_output += "\n\n[input_colums]"
			for col_item in in_columns:
				in_col = col_item["col_info"]
				col_id, col_name = in_col.col_id, in_col.name
				if col_name not in column_data[s_file1]:
					raise Exception("error: in_col_name={} not found in s_file1={}".format(col_name, s_file1))
				col_size_B = column_data[s_file1][col_name]["data_files"]["data_file"]["size_B"]
				in_size_B += col_size_B
				ccs_output += "\ncol_id={}, col_name={}, size={}, in_col={}".format(in_col.col_id, col_name, sizeof_fmt(col_size_B), in_col)

			# out_cols
			out_size_B = 0
			ccs_output += "\n[output_colums]"
			for col_item in out_columns:
				out_col = col_item["col_info"]
				# handle expcetion columns separately
				if OutputColumnManager.is_exception_col(out_col):
					continue
				col_id, col_name = out_col.col_id, out_col.name
				if col_name not in column_data[s_file2]:
					raise Exception("out_col_name={} not found in s_file2={}".format(col_name, s_file2))
				if out_columns_stats and col_id in out_columns_stats:
					null_ratio = out_columns_stats[col_id]["null_ratio"]
				else:
					null_ratio = "N/A"
				col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
				out_size_B += col_size_B
				ccs_output += "\ncol_id={}, col_name={}, null_ratio={}, size={}, out_col={}".format(out_col.col_id, col_name, null_ratio, sizeof_fmt(col_size_B), out_col)

			# exception columns
			ex_size_B = 0
			ccs_output += "\n[exception_colums]"
			for col_item in out_columns:
				ex_col = col_item["col_info"]
				if not OutputColumnManager.is_exception_col(ex_col):
					continue
				col_id, col_name = ex_col.col_id, ex_col.name
				if col_name not in column_data[s_file2]:
					raise Exception("ex_col_name={} not found in s_file2={}".format(col_name, s_file2))
				if out_columns_stats and col_id in out_columns_stats:
					null_ratio = out_columns_stats[col_id]["null_ratio"]
				else:
					null_ratio = "N/A"
				col_size_B = column_data[s_file2][col_name]["data_files"]["data_file"]["size_B"]
				ex_size_B += col_size_B
				ccs_output += "\ncol_id={}, col_name={}, null_ratio={}, size={}, ex_col={}".format(ex_col.col_id, col_name, null_ratio, sizeof_fmt(col_size_B), ex_col)

			# metadata
			ccs_output += "\n[metadata]"
			metadata_size_B = get_metadata_size(cc_expr_tree)
			ccs_output += "\nmetadata_size={}".format(sizeof_fmt(metadata_size_B))

			# summary
			total_out_size_B = out_size_B + ex_size_B + metadata_size_B
			compression_ratio = float(in_size_B) / total_out_size_B if total_out_size_B > 0 else float("inf")
			ccs_output += "\n[summary] {}".format([(idx, cc_expr_tree.get_node(n_id).p_id) for idx, l in enumerate(cc_expr_tree.levels) for n_id in l])
			ccs_output += "\ncc={cc}, compression_ratio={compression_ratio:.2f}, in_size={in_size}, total_out_size={total_out_size} (out_size={out_size}, ex_size={ex_size}, metadata_size={metadata_size})".format(
					cc=cc_expr_tree.get_in_columns(),
					compression_ratio=compression_ratio,
					in_size=sizeof_fmt(in_size_B),
					total_out_size=sizeof_fmt(total_out_size_B),
					out_size=sizeof_fmt(out_size_B),
					ex_size=sizeof_fmt(ex_size_B),
					metadata_size=sizeof_fmt(metadata_size_B)
				)

			agg_in_size_B += in_size_B
			agg_total_out_size_B += total_out_size_B

		# aggregate for used columns
		agg_compression_ratio = float(agg_in_size_B) / agg_total_out_size_B if agg_total_out_size_B > 0 else float("inf")
		agg_output += "\n[{table_name}] agg_in_size={agg_in_size_B}, agg_total_out_size={agg_total_out_size_B}, used_compression_ratio={agg_compression_ratio:.2f}".format(
					table_name=table_name,
					agg_in_size_B=sizeof_fmt(agg_in_size_B),
					agg_total_out_size_B=sizeof_fmt(agg_total_out_size_B),
					agg_compression_ratio=agg_compression_ratio)

	output = agg_output + "\n" + ccs_output
	return output


def compare_stats(s_file1, s_file2, expr_tree_file, apply_expr_stats_file):
	with open(s_file1, 'r') as f1, open(s_file2, 'r') as f2:
		s_data1 = json.load(f1)
		s_data2 = json.load(f2)

	try:
		table_name = os.path.basename(s_file1).split(".")[0]
	except Exception as e:
		print("debug: unable to get table name from file name")
		table_name = "<table_name>"

	# data_files
	try:
		output_df = compare_data_files(s_file1, s_file2, s_data1, s_data2, expr_tree_file, table_name)
		print(output_df)
	except Exception as e:
		print("error: unable to perform: compare_data_files; e={}".format(e))

	# expression nodes: in/out column comparison
	try:
		output_cols = compare_ccs(s_file1, s_file2, s_data1, s_data2, expr_tree_file, apply_expr_stats_file, table_name)
		print(output_cols)
	except Exception as e:
		print("error: unable to perform: compare_ccs; e={}".format(e))


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
    out_table="${table}_out"; \
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
