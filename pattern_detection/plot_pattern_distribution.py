#!/usr/bin/env python3

import os, sys
import argparse
import numpy as np


def plot(plt, header, rows, out_file=None, out_file_format="svg"):
	arr = np.array(rows)

	plt.figure(figsize=(8, 8), dpi=100)
	plt.imshow(arr, aspect='auto')
	plt.xticks(range(len(header)), header, rotation=128)
	plt.xlabel("pattern")
	plt.ylabel("row")
	plt.tight_layout()

	if out_file:
		plt.savefig(out_file, format=out_file_format)
	else:
		plt.show()


def read_data(input_file):
	rows = []
	with open(input_file, 'r') as fd:
		header = fd.readline().split(",")
		for line in fd:
			rows.append(list(map(int, line.split(","))))
	return header, rows


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Plot pattern distribution accross the rows of a column."""
	)

	parser.add_argument('file', help='Pattern distribution file')
	parser.add_argument('--out-file', dest='out_file', type=str,
		help="Output file to save plot to")
	parser.add_argument('--out-file-format', dest='out_file_format', type=str,
		help="Format of the ouput file", default="svg")

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	# NOTE: this is needed when running on remote server through ssh
	# see: https://stackoverflow.com/questions/4706451/how-to-save-a-figure-remotely-with-pylab
	if args.out_file is not None:
		import matplotlib
		matplotlib.use('Agg')
	import matplotlib.pyplot as plt

	header, rows = read_data(args.file)
	plot(plt, header, rows, out_file=args.out_file, out_file_format=args.out_file_format)


if __name__ == "__main__":
	main()


"""
# input_file=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test/CommonGovernment/CommonGovernment_1.patterns/col_48.csv
input_file=/ufs/bogdan/work/master-project/public_bi_benchmark-analysis/pattern_detection/output/col_48.csv
output_file_format=svg
output_file=/ufs/bogdan/work/master-project/public_bi_benchmark-analysis/pattern_detection/output/col_48.$output_file_format

./pattern_detection/plot_pattern_distribution.py $input_file --out-file $output_file --out-file-format $output_file_format
"""
