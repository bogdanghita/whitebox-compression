#!/usr/bin/env python3

import os
import sys
import argparse
import json
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
			pd_info = {
				"name": pd.name,
				"columns": []
			}
			for col in self.columns:
				if pd.name in col.patterns and len(col.patterns[pd.name]["rows"]) > 0:
					pd_info["columns"].append({
						"column": repr(col),
						"rows": col.patterns[pd.name]["rows"],
						"percentage": (len(col.patterns[pd.name]["rows"]) / pd.row_count * 100) if pd.row_count > 0 else 0
					})
			# if len(pd_info["columns"]) > 0:
			patterns[pd.name] = pd_info

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


def output_patterns(patterns):
	# print(json.dumps(patterns, indent=2))
	for p in patterns.values():
		print("*** {} ***".format(p["name"]))
		for c in sorted(p["columns"], key=lambda x: x["percentage"], reverse=True):
			print("{}\t{}".format(c["percentage"], c["column"]))


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
		NumberAsString(columns, args.null),
		StringCommonPrefix(columns, args.null)
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

	output_patterns(patterns)


if __name__ == "__main__":
	main()


"""
wbs_dir=/ufs/bogdan/work/master-project/public_bi_benchmark-master_project/benchmark

./main.py --header-file $wbs_dir/Arade/samples/Arade_1.header.csv --datatypes-file $wbs_dir/Arade/samples/Arade_1.datatypes.csv $wbs_dir/Arade/samples/Arade_1.sample.csv

./main.py --header-file $wbs_dir/CommonGovernment/samples/CommonGovernment_1.header.csv --datatypes-file $wbs_dir/CommonGovernment/samples/CommonGovernment_1.datatypes.csv $wbs_dir/CommonGovernment/samples/CommonGovernment_1.sample.csv

./main.py --header-file $wbs_dir/Eixo/samples/Eixo_1.header.csv --datatypes-file $wbs_dir/Eixo/samples/Eixo_1.datatypes.csv $wbs_dir/Eixo/samples/Eixo_1.sample.csv
"""
