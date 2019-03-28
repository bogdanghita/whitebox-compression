#!/usr/bin/env python3

import os
import sys
import argparse
import json
import string
from lib.util import *
from patterns import *


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

		return l.strip()


def driver_loop(driver, pd_engine, fdelim):
	while True:
		line = driver.nextTuple()
		if line is None:
			break

		tpl = line.split(fdelim)
		pd_engine.feed_tuple(tpl)


def output_patterns(columns, patterns):
	# print(json.dumps(patterns, indent=2))
	# TODO: also output percentage of NULLs on each columns
	for pd in patterns.values():
		print("*** {} ***".format(pd["name"]))
		print("# TODO: also output percentage of NULLs on each column")
		for col_id, col_p_list in pd["columns"].items():
			print("{}".format(columns[col_id]))
			for p in sorted(col_p_list, key=lambda x: x["score"], reverse=True):
				print("{:.2f}\t{}".format(p["score"], p["p_id"]))


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Detect column patterns in CSV file."""
	)

	parser.add_argument('file', metavar='FILE', nargs='?',
		help='CSV file to process. Stdin if none given')
	parser.add_argument('--header-file', dest='header_file', type=str,
		help="CSV file containing the header row (<workbook>/samples/<table>.header.csv)",
		required=True)
	parser.add_argument('--datatypes-file', dest='datatypes_file', type=str,
		help="CSV file containing the datatypes row (<workbook>/samples/<table>.datatypes.csv)",
		required=True)
	parser.add_argument("-F", "--fdelim", dest="fdelim",
		help="Use <fdelim> as delimiter between fields", default="|")
	parser.add_argument("--null", dest="null", type=str,
		help="Interprets <NULL> as NULLs", default="null")

	return parser.parse_args()


def main():
	args = parse_args()

	with open(args.header_file, 'r') as fd:
		header = list(map(lambda x: x.strip(), fd.readline().split(args.fdelim)))
	with open(args.datatypes_file, 'r') as fd:
		datatypes = list(map(lambda x: x.strip(), fd.readline().split(args.fdelim)))
	if len(header) != len(datatypes):
		return RET_ERR

	columns = []
	for col_id, col_name in enumerate(header):
		columns.append(Column(col_id, col_name, datatypes[col_id]))

	pattern_detectors = [
		# NumberAsString(columns, args.null),
		# StringCommonPrefix(columns, args.null),
		CharSetSplit(columns, args.null, default_placeholder="?", char_sets=[
			{"name": "digits", "placeholder": "D", "char_set": set(map(str, range(0,10)))},
			# {"name": "letters", "placeholder": "L", "char_set": set(string.ascii_lowercase + string.ascii_uppercase)},
			# TODO: play around with the char sets here
		]),
		# TODO: add new pattern detectors here
	]
	pd_engine = PatternDetectionEngine(columns, pattern_detectors)

	try:
		if args.file is None:
			fd = os.fdopen(os.dup(sys.stdin.fileno()))
		else:
			fd = open(args.file, 'r')
		driver = FileDriver(fd, args.fdelim)
		driver_loop(driver, pd_engine, args.fdelim)
	finally:
		fd.close()

	patterns = pd_engine.get_patterns()

	output_patterns(columns, patterns)


if __name__ == "__main__":
	main()


"""
wbs_dir=/ufs/bogdan/work/master-project/public_bi_benchmark-master_project/benchmark

./main.py --header-file $wbs_dir/Arade/samples/Arade_1.header.csv --datatypes-file $wbs_dir/Arade/samples/Arade_1.datatypes.csv $wbs_dir/Arade/samples/Arade_1.sample.csv

./main.py --header-file $wbs_dir/CommonGovernment/samples/CommonGovernment_1.header.csv --datatypes-file $wbs_dir/CommonGovernment/samples/CommonGovernment_1.datatypes.csv $wbs_dir/CommonGovernment/samples/CommonGovernment_1.sample.csv

./main.py --header-file $wbs_dir/Eixo/samples/Eixo_1.header.csv --datatypes-file $wbs_dir/Eixo/samples/Eixo_1.datatypes.csv $wbs_dir/Eixo/samples/Eixo_1.sample.csv

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
./pattern_detection/main.py --header-file $repo_wbs_dir/$wb/samples/$table.header.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv $wbs_dir/$wb/$table.sample.csv
"""
