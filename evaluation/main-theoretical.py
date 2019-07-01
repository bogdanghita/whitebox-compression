#!/usr/bin/env python3

import os
import sys
import argparse
import json
from lib.util import *
import theoretical_evaluation


def aggregate_stats(schema, stats):
	res = {}

	table_size_B = 0
	res_columns = {col_id: {"col_data": col_data} for col_id, col_data in schema.items()}

	for col_id, estimators in stats.items():
		if col_id not in res_columns:
			print("debug: unknown col_id: {}".format(col_id))
			continue

		if len(estimators.keys()) == 0:
			raise Exception("No estimator result for col_id: {}".format(col_id))

		best_estimator_name = min(estimators.keys(), key=lambda name: estimators[name]["size_B"])
		# best_estimator_name = "NoCompressionEstimator"
		size_B = estimators[best_estimator_name]["size_B"]

		table_size_B += size_B
		res_columns[col_id]["data_files"] = {
			"data_file": {
				"size_B": size_B,
				"size_human_readable": sizeof_fmt(size_B),
				"compression": best_estimator_name,
				"estimators": estimators
			}
		}

	res_agg = {
		"data_files": {
			"size_B": table_size_B,
			"size_human_readable": sizeof_fmt(table_size_B)
		}
	}

	res = {
		"columns": res_columns,
		"table": res_agg
	}
	return res


def output_agg_stats(agg_stats, table_name, output_dir):
	out_f = os.path.join(output_dir, "{}.eval-theoretical.json".format(table_name))
	with open(out_f, 'w') as f:
		json.dump(agg_stats, f, indent=2)


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Detect column patterns in CSV file."""
	)

	# parser.add_argument('file', help='CSV file to process')
	parser.add_argument('--train-file', dest='train_file', type=str,
		help="CSV file for training", required=True)
	parser.add_argument('--test-file', dest='test_file', type=str,
		help="CSV file for testing", required=True)
	parser.add_argument('--schema-file', dest='schema_file', type=str,
		help="SQL file containing table schema", required=True)
	parser.add_argument('--table-name', dest='table_name', type=str,
		help="Name of the table", required=True)
	parser.add_argument('--output-dir', dest='output_dir', type=str,
		help="Output directory to dump result files to", required=True)
	parser.add_argument('--full-file-linecount', dest='full_file_linecount', type=int,
		help="Number of lines in the full file that the sample was taken from", required=True)
	parser.add_argument("-F", "--fdelim", dest="fdelim",
		help="Use <fdelim> as delimiter between fields", default="|")
	parser.add_argument("--null", dest="null", type=str,
		help="Interprets <NULL> as NULLs", default="null")
	parser.add_argument('--no-compression', dest='no_compression', action='store_true',
		help="Estimate uncompressed size")

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	schema = parse_schema_file(args.schema_file)

	stats = theoretical_evaluation.main(schema, args.train_file, args.test_file, args.full_file_linecount,
										args.fdelim, args.null, args.no_compression)

	agg_stats = aggregate_stats(schema, stats)

	output_agg_stats(agg_stats, args.table_name, args.output_dir)


if __name__ == "__main__":
	main()


"""
#[remote]
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=/scratch/bogdan/master-project/public_bi_benchmark-master_project/benchmark
#[local]
wbs_dir=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1
repo_wbs_dir=/ufs/bogdan/work/master-project/public_bi_benchmark-master_project/benchmark

================================================================================
wb=Arade
table=Arade_1
================================================================================
wb=CommonGovernment
table=CommonGovernment_1
================================================================================
wb=Eixo
table=Eixo_1
================================================================================
wb=Generico
table=Generico_2


================================================================================
# no-compression
table_name=$table
output_dir=$wbs_dir/$wb/$table.evaluation-nocompression
train_file=$wbs_dir/$wb/$table.sample-theoretical-train.csv
# test_file=$wbs_dir/$wb/$table.sample-theoretical-test.csv
test_file=$wbs_dir/$wb/$table.csv
schema_file=$repo_wbs_dir/$wb/tables-vectorwise/$table.table-renamed.sql
full_file_linecount=$repo_wbs_dir/$wb/samples/$table.linecount
no_compression="--no-compression"

# default
table_name=$table
output_dir=$wbs_dir/$wb/$table.evaluation
train_file=$wbs_dir/$wb/$table.sample-theoretical-train.csv
# test_file=$wbs_dir/$wb/$table.sample-theoretical-test.csv
test_file=$wbs_dir/$wb/$table.csv
schema_file=$repo_wbs_dir/$wb/tables-vectorwise/$table.table-renamed.sql
full_file_linecount=$repo_wbs_dir/$wb/samples/$table.linecount
no_compression=""

# whitebox-compression
out_table=${table}_out
table_name=$out_table
output_dir=$wbs_dir/$wb/$table.poc_1_out-theoretical
train_file=$output_dir/$out_table-train.csv
test_file=$output_dir/$out_table-test.csv
schema_file=$output_dir/$out_table.table.sql
full_file_linecount=$repo_wbs_dir/$wb/samples/$table.linecount
no_compression=""

mkdir -p $output_dir
time ./evaluation/main-theoretical.py \
--schema-file $schema_file \
--table-name $table_name \
--output-dir $output_dir \
$no_compression \
--full-file-linecount $(cat $full_file_linecount) \
--train-file $train_file \
--test-file $test_file

# no-compression results
cat $wbs_dir/$wb/${table}.evaluation-nocompression/${table}.eval-theoretical.json | less -S
# default results
cat $wbs_dir/$wb/${table}.evaluation/${table}.eval-theoretical.json | less -S
# whitebox-compression results
cat $wbs_dir/$wb/${table}.poc_1_out-theoretical/${table}_out.eval-theoretical.json | less -S
"""
