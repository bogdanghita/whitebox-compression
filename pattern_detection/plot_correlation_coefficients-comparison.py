#!/usr/bin/env python3

import os, sys
import argparse
import numpy as np

"""
def plot(plt, data, data_cramersv, data_theilsu, out_file=None, out_file_format="svg"):
	# rows = list(map(list, zip(*rows))) # transpose

	plt.figure()
	# figsize = max(8, len(header) / 6)
	# plt.figure(figsize=(figsize, figsize), dpi=100)
	
	# aspect_ratio = 'auto'
	aspect_ratio = 1.5
	label_fontsize_1, label_fontsize_2 = 16, 14

	# data_cramersv
	header, rows = data_cramersv
	arr = np.array(rows)
	ax1 = plt.subplot(1, 3, 1)
	plt.imshow(arr, cmap='hot', interpolation=None, aspect=aspect_ratio)
	plt.xticks(range(len(header)), header, rotation=270)
	plt.yticks(range(len(header)), header, rotation=0)
	plt.ylabel("col_id (determines cols on X axis)")
	plt.title("Cramer's V")

	# data_theilsu
	header, rows = data_theilsu
	arr = np.array(rows)
	plt.subplot(1, 3, 2)
	plt.imshow(arr, cmap='hot', interpolation=None, aspect=aspect_ratio)
	plt.xticks(range(len(header)), header, rotation=270)
	plt.yticks(range(len(header)), [])
	plt.xlabel("col_id (is determined by cols on Y axis)")
	plt.title("Theil's U")

	# data
	header, rows = data
	arr = np.array(rows)
	plt.subplot(1, 3, 3)
	plt.imshow(arr, cmap='hot', interpolation=None, aspect=aspect_ratio)
	cbar = plt.colorbar()
	cbar.set_label(label="correlation coefficient")
	plt.xticks(range(len(header)), header, rotation=270)
	plt.yticks(range(len(header)), [])
	plt.title("TODO")

	# plt.tight_layout()

	if out_file:
		plt.savefig(out_file, bbox_inches='tight', format=out_file_format)
	else:
		plt.show()

	plt.close()
"""

def plot(plt, data, data_cramersv, data_theilsu, out_file=None, out_file_format="svg"):
	plt.figure()
	
	aspect_ratio = 'auto'
	# aspect_ratio = 1
	ticks_fontsize, labels_fontsize, cbar_fontsize = 10, 12, 11

	fig, axes = plt.subplots(1, 3, sharey=True, figsize=(7.4, 3.3))
	(ax1, ax2, ax3) = axes

	# data_cramersv
	header, rows = data_cramersv
	arr = np.array(rows)
	ax1.imshow(arr, cmap='hot', interpolation=None, aspect=aspect_ratio)
	ax1.set_xticks(range(len(header)))
	ax1.set_xticklabels(header, rotation=270, fontsize=ticks_fontsize)
	ax1.set_yticks(range(len(header)))
	ax1.set_yticklabels(header, rotation=0, fontsize=ticks_fontsize)
	ax1.set_ylabel("col_id (determines cols on X axis)", fontsize=labels_fontsize)
	ax1.set_title("Cramer's V", fontsize=labels_fontsize)

	# data_theilsu
	header, rows = data_theilsu
	arr = np.array(rows)
	ax2.imshow(arr, cmap='hot', interpolation=None, aspect=aspect_ratio)
	ax2.set_xticks(range(len(header)))
	ax2.set_xticklabels(header, rotation=270, fontsize=ticks_fontsize)
	ax2.set_xlabel("col_id (is determined by cols on Y axis)", fontsize=labels_fontsize)
	ax2.set_title("Theil's U", fontsize=labels_fontsize)

	# data
	header, rows = data
	arr = np.array(rows)
	im3 = ax3.imshow(arr, cmap='hot', interpolation=None, aspect=aspect_ratio)
	ax3.set_xticks(range(len(header)))
	ax3.set_xticklabels(header, rotation=270, fontsize=ticks_fontsize)
	ax3.set_title("1 - exception_ratio", fontsize=labels_fontsize)

	fig.subplots_adjust(right=1.2)
	cbar = fig.colorbar(im3, ax=axes.ravel().tolist())
	cbar.set_label(label="correlation coefficient", fontsize=labels_fontsize)
	cbar.ax.tick_params(labelsize=cbar_fontsize)

	# plt.tight_layout()

	if out_file:
		plt.savefig(out_file, bbox_inches='tight', format=out_file_format)
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
	parser.add_argument('--cramersv', dest='cramersv_file', type=str,
		help="Cramer's V results")
	parser.add_argument('--theilsu', dest='theilsu_file', type=str,
		help="Theil's U results")
	parser.add_argument('--out-file', dest='out_file', type=str,
		help="Output file to save plot to")
	parser.add_argument('--out-file-format', dest='out_file_format', type=str,
		help="Format of the ouput file", default="svg")

	return parser.parse_args()


def main(in_file, cramersv_file, theilsu_file, out_file, out_file_format):
	# NOTE: this is needed when running on remote server through ssh
	# see: https://stackoverflow.com/questions/4706451/how-to-save-a-figure-remotely-with-pylab
	if out_file is not None:
		import matplotlib
		matplotlib.use('Agg')
	import matplotlib.pyplot as plt

	data = read_data(in_file)
	data_cramersv = read_data(cramersv_file)
	data_theilsu = read_data(theilsu_file)
	plot(plt, data, data_cramersv, data_theilsu, out_file=out_file, out_file_format=out_file_format)


if __name__ == "__main__":
	args = parse_args()
	print(args)

	main(args.file, args.cramersv_file, args.theilsu_file, args.out_file, args.out_file_format)


"""
input_file_1=/media/bogdan/Data/Bogdan/Work/cwi-data/tableau-public-bench/data/PublicBIbenchmark-poc_1/YaleLanguages/YaleLanguages_1.corr_coefs/cv-s_1_l_0.coefs.csv
input_file_2=/media/bogdan/Data/Bogdan/Work/cwi-data/tableau-public-bench/data/PublicBIbenchmark-poc_1/YaleLanguages/YaleLanguages_1.corr_coefs/tu-s_1_l_0.coefs.csv
input_file_3=/media/bogdan/Data/Bogdan/Work/cwi-data/tableau-public-bench/data/PublicBIbenchmark-poc_1/YaleLanguages/YaleLanguages_1.corr_coefs/bg-s_1_l_0.coefs.csv
output_file_format=svg
output_file=/media/bogdan/Data/Bogdan/Work/cwi-data/tableau-public-bench/data/PublicBIbenchmark-poc_1/YaleLanguages/YaleLanguages_1.corr_coefs/merged-s_1_l_0.coefs.$output_file_format

./pattern_detection/plot_correlation_coefficients-comparison.py $input_file_3 --cramersv $input_file_1 --theilsu $input_file_2 --out-file $output_file --out-file-format $output_file_format
"""
