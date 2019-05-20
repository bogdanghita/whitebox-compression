#!/usr/bin/env python3

import os, sys
import argparse
import numpy as np


def plot(plt, header, rows, out_file=None, out_file_format="svg"):
	# rows = list(map(list, zip(*rows))) # transpose
	arr = np.array(rows)

	# plt.figure(figsize=(8, 8), dpi=100)
	figsize = max(8, len(header) / 6)
	plt.figure(figsize=(figsize, figsize), dpi=100)
	plt.imshow(arr, cmap='hot', interpolation=None, aspect='auto')
	plt.colorbar(label="correlation coefficient")
	plt.xticks(range(len(header)), header, rotation=270)
	plt.yticks(range(len(header)), header, rotation=0)
	plt.xlabel("col_id (is determined by cols on Y axis)")
	plt.ylabel("col_id (determines cols on X axis)")
	plt.tight_layout()

	if out_file:
		plt.savefig(out_file, format=out_file_format)
	else:
		plt.show()

	plt.close()


def read_data(input_file):
	rows = []
	with open(input_file, 'r') as fd:
		header = fd.readline().split(",")
		for line in fd:
			rows.append(list(map(float, line.split(","))))
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


def main(in_file, out_file, out_file_format):
	# NOTE: this is needed when running on remote server through ssh
	# see: https://stackoverflow.com/questions/4706451/how-to-save-a-figure-remotely-with-pylab
	if out_file is not None:
		import matplotlib
		matplotlib.use('Agg')
	import matplotlib.pyplot as plt

	header, rows = read_data(in_file)
	plot(plt, header, rows, out_file=out_file, out_file_format=out_file_format)


if __name__ == "__main__":
	args = parse_args()
	print(args)

	main(args.file, args.out_file, args.out_file_format)


"""
input_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.corr_coefs/l_0.csv
output_file_format=svg
output_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.corr_coefs/l_0.$output_file_format

./pattern_detection/plot_correlation_coefficients.py $input_file --out-file $output_file --out-file-format $output_file_format
"""
