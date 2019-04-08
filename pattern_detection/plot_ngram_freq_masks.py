#!/usr/bin/env python3

import os, sys
import argparse
import numpy as np

MAX_INT = sys.maxsize
MIN_INT = -sys.maxsize - 1


def plot(plt, rows, out_file=None, out_file_format="svg"):
	max_row_length = MIN_INT
	min_freq_value, max_freq_value = MAX_INT, MIN_INT
	for r in rows:
		if len(r) > 0:
			min_v, max_v = min(r), max(r)
			if min_v < min_freq_value:
				min_freq_value = min_v
			if max_v > max_freq_value:
				max_freq_value = max_v
		r_l = len(r)
		if r_l > max_row_length:
			max_row_length = r_l
	padding_value = min_freq_value - ((max_freq_value - min_freq_value) * 0.25)
	for r in rows:
		r.extend([padding_value] * (max_row_length - len(r)))
		print(r)

	arr = np.array(rows)

	plt.figure(figsize=(8, 8), dpi=100)
	plt.imshow(arr, cmap='hot', interpolation='none', aspect='auto')
	plt.colorbar(boundaries=range(int(min_freq_value), int(max_freq_value)), label="ngram frequency")
	plt.xlabel("ngram start index in string")
	plt.ylabel("row")
	plt.tight_layout()

	if out_file:
		plt.savefig(out_file, format=out_file_format)
	else:
		plt.show()

	plt.close()


def read_data(input_file):
	rows = []
	with open(input_file, 'r') as fd:
		for line in fd:
			if line == "\n":
				# NOTE: uncomment if you want to plot empty lines
				# rows.append([])
				pass
			else:
				rows.append(list(map(int, line.split(","))))
	return rows

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

	rows = read_data(in_file)
	plot(plt, rows, out_file=out_file, out_file_format=out_file_format)


if __name__ == "__main__":
	args = parse_args()
	print(args)

	main(args.file, args.out_file, args.out_file_format)


"""
# input_file=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test/Eixo/Eixo_1.ngram_freq_masks/col_28.csv
input_file=/ufs/bogdan/work/master-project/public_bi_benchmark-analysis/pattern_detection/output/col_28.csv
output_file_format=svg
output_file=/ufs/bogdan/work/master-project/public_bi_benchmark-analysis/pattern_detection/output/col_28.$output_file_format

./pattern_detection/plot_ngram_freq_masks.py $input_file --out-file $output_file --out-file-format $output_file_format
"""
