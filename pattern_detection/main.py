#!/usr/bin/env python3

import os
import sys
import argparse
import json
import string
from lib.util import *
from patterns import *
import plot_pattern_distribution, plot_ngram_freq_masks


RET_ERR = 15


class PatternDetectionEngine(object):
	def __init__(self, columns, pattern_detectors):
		self.columns = columns
		self.pattern_detectors = pattern_detectors
		self.total_tuple_count = 0
		self.valid_tuple_count = 0

	def is_valid_tuple(self, tpl):
		if len(tpl) != len(self.columns):
			return False
		return True

	def feed_tuple(self, tpl):
		self.total_tuple_count += 1

		if not self.is_valid_tuple(tpl):
			return False
		self.valid_tuple_count += 1

		for pd in self.pattern_detectors:
			pd.feed_tuple(tpl)

	def get_patterns(self):
		patterns = {}

		for pd in self.pattern_detectors:
			patterns[pd.name] = {
				"name": pd.name,
				"columns": pd.evaluate()
			}

		return patterns


class FileDriver(object):
	def __init__(self, fd, args):
		self.fd = fd
		self.done = False

	def nextTuple(self):
		if self.done:
			return None

		l = self.fd.readline()
		if not l:
			self.done = True
			return None

		return l.rstrip('\r\n')


def driver_loop(driver, pd_engine, fdelim):
	while True:
		line = driver.nextTuple()
		if line is None:
			break

		tpl = line.split(fdelim)
		pd_engine.feed_tuple(tpl)


def output_stats(columns, patterns):
	# print(json.dumps(patterns, indent=2))
	for pd in patterns.values():
		print("*** {} ***".format(pd["name"]))
		for col_id, col_p_list in pd["columns"].items():
			print("{}".format(columns[col_id]))
			for p in sorted(col_p_list, key=lambda x: x["score"], reverse=True):
				print("{:.2f}\t{}, res_columns={}, operator_info={}".format(p["score"], p["p_id"], p["res_columns"], p["operator_info"]))


def output_pattern_distribution(columns, patterns, pattern_distribution_output_dir, fdelim=",", plot_file_format="svg"):
	# group patterns by columns
	column_patterns = {}
	for c in columns:
		column_patterns[c.col_id] = {}
	for pd in patterns.values():
		for col_id, col_p_list in pd["columns"].items():
			for p in col_p_list:
				column_patterns[col_id][p["p_id"]] = p

	# output pattern distributions
	for col_id, col_p in column_patterns.items():
		if len(col_p.keys()) == 0:
			continue

		out_file = "{}/col_{}.csv".format(pattern_distribution_output_dir, col_id)
		with open(out_file, 'w') as fd:
			# write header
			header = sorted(col_p.keys())
			fd.write(fdelim.join(header) + "\n")

			# write one row at a time
			# NOTE: we assume that col_p[p_id]["rows"] is sorted in increasing order
			row_iterators = {p:0 for p in col_p.keys()}
			row_count = 1
			while True:
				current_row = []
				done_cnt = 0
				for p in header:
					rows, r_it = col_p[p]["rows"], row_iterators[p]
					if r_it == len(rows):
						current_row.append("0")
						done_cnt += 1
					elif row_count < rows[r_it]:
						current_row.append("0")
					elif row_count == rows[r_it]:
						row_iterators[p] += 1
						current_row.append("1")
					else:
						raise Exception("Rows are not sorted: col_id={}, p={}, row_count={}, rows[{}]={}".format(col_id, p, row_count, r_it, rows[r_it]))
				if done_cnt == len(header):
					break
				fd.write(fdelim.join(current_row) + "\n")
				row_count += 1

		plot_file="{}/col_{}.{}".format(pattern_distribution_output_dir, col_id, plot_file_format)
		plot_pattern_distribution.main(in_file=out_file, out_file=plot_file, out_file_format=plot_file_format)


def output_ngram_freq_masks(ngram_freq_masks, ngram_freq_masks_output_dir, plot_file_format="svg"):
	for col_id, values in ngram_freq_masks.items():
		out_file = "{}/col_{}.csv".format(ngram_freq_masks_output_dir, col_id)
		with open(out_file, 'w') as fd:
			for v in values:
				fd.write(v + "\n")

		plot_file="{}/col_{}.{}".format(ngram_freq_masks_output_dir, col_id, plot_file_format)
		plot_ngram_freq_masks.main(in_file=out_file, out_file=plot_file, out_file_format=plot_file_format)


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
	parser.add_argument('--pattern-distribution-output-dir', dest='pattern_distribution_output_dir', type=str,
		help="Output file to write pattern distribution to")
	parser.add_argument('--ngram-freq-masks-output-dir', dest='ngram_freq_masks_output_dir', type=str,
		help="Output file to write ngram frequency masks to")
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

	columns = []
	for col_id, col_name in enumerate(header):
		columns.append(Column(col_id, col_name, datatypes[col_id]))

	# pattern detectors
	char_set_split = CharSetSplit(columns, args.null, default_placeholder="?", char_sets=[
		{"name": "digits", "placeholder": "D", "char_set": set(map(str, range(0,10)))},
		# {"name": "letters", "placeholder": "L", "char_set": set(string.ascii_lowercase + string.ascii_uppercase)},
		# TODO: play around with the char sets here
	])
	ngram_freq_split = NGramFreqSplit(columns, args.null, n=3)
	pattern_detectors = [
		# NullPatternDetector(columns, args.null),
		# ConstantPatternDetector(columns, args.null),
		NumberAsString(columns, args.null),
		# StringCommonPrefix(columns, args.null),
		char_set_split,
		# ngram_freq_split,
		# NOTE: add new pattern detectors here
	]
	pd_engine = PatternDetectionEngine(columns, pattern_detectors)

	# feed data to engine
	try:
		if args.file is None:
			fd = os.fdopen(os.dup(sys.stdin.fileno()))
		else:
			fd = open(args.file, 'r')
		driver = FileDriver(fd, args.fdelim)
		driver_loop(driver, pd_engine, args.fdelim)
	finally:
		fd.close()

	# get results from engine
	patterns = pd_engine.get_patterns()

	# otuput results
	output_stats(columns, patterns)
	if args.pattern_distribution_output_dir is not None:
		output_pattern_distribution(columns, patterns, args.pattern_distribution_output_dir)
	if args.ngram_freq_masks_output_dir is not None:
		ngram_freq_masks = ngram_freq_split.get_ngram_freq_masks(delim=",")
		output_ngram_freq_masks(ngram_freq_masks, args.ngram_freq_masks_output_dir)


if __name__ == "__main__":
	main()


"""
wbs_dir=/ufs/bogdan/work/master-project/public_bi_benchmark-master_project/benchmark

./main.py --header-file $wbs_dir/Arade/samples/Arade_1.header-renamed.csv --datatypes-file $wbs_dir/Arade/samples/Arade_1.datatypes.csv $wbs_dir/Arade/samples/Arade_1.sample.csv

./main.py --header-file $wbs_dir/CommonGovernment/samples/CommonGovernment_1.header-renamed.csv --datatypes-file $wbs_dir/CommonGovernment/samples/CommonGovernment_1.datatypes.csv $wbs_dir/CommonGovernment/samples/CommonGovernment_1.sample.csv

./main.py --header-file $wbs_dir/Eixo/samples/Eixo_1.header-renamed.csv --datatypes-file $wbs_dir/Eixo/samples/Eixo_1.datatypes.csv $wbs_dir/Eixo/samples/Eixo_1.sample.csv

================================================================================
*** CommonGovernment/CommonGovernment_1 ***

wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=/scratch/bogdan/master-project/public_bi_benchmark-master_project/benchmark
wb=CommonGovernment
table=CommonGovernment_1

#[sample]
max_sample_size=$((1024*1024*10))
dataset_nb_rows=$(cat $repo_wbs_dir/$wb/samples/$table.linecount)
./sampling/main.py --dataset-nb-rows $dataset_nb_rows --max-sample-size $max_sample_size --sample-block-nb-rows 32 --output-file $wbs_dir/$wb/$table.sample.csv $wbs_dir/$wb/$table.csv

#[pattern-detection]
pattern_distr_out_dir=$wbs_dir/$wb/$table.patterns
mkdir -p $pattern_distr_out_dir
./pattern_detection/main.py --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv --pattern-distribution-output-dir $pattern_distr_out_dir $wbs_dir/$wb/$table.sample.csv

#[scp-pattern-detection-results]
scp -r bogdan@bricks14:/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test/CommonGovernment/CommonGovernment_1.patterns pattern_detection/output/

================================================================================
*** Eixo/Eixo_1 ***

wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=/scratch/bogdan/master-project/public_bi_benchmark-master_project/benchmark
wb=Eixo
table=Eixo_1

#[sample]
max_sample_size=$((1024*1024*10))
dataset_nb_rows=$(cat $repo_wbs_dir/$wb/samples/$table.linecount)
./sampling/main.py --dataset-nb-rows $dataset_nb_rows --max-sample-size $max_sample_size --sample-block-nb-rows 64 --output-file $wbs_dir/$wb/$table.sample.csv $wbs_dir/$wb/$table.csv

#[pattern-detection]
pattern_distr_out_dir=$wbs_dir/$wb/$table.patterns
ngram_freq_masks_output_dir=$wbs_dir/$wb/$table.ngram_freq_masks
mkdir -p $pattern_distr_out_dir $ngram_freq_masks_output_dir
./pattern_detection/main.py --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv --pattern-distribution-output-dir $pattern_distr_out_dir --ngram-freq-masks-output-dir $ngram_freq_masks_output_dir $wbs_dir/$wb/$table.sample.csv

#[scp-pattern-detection-results]
scp -r bogdan@bricks14:/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test/Eixo/Eixo_1.patterns pattern_detection/output/
scp -r bogdan@bricks14:/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test/Eixo/Eixo_1.ngram_freq_masks pattern_detection/output/
"""
