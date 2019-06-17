#!/usr/bin/env python3

import os, sys
import argparse
import numpy as np
import json

# NOTE: this is needed when running on remote server through ssh
# see: https://stackoverflow.com/questions/4706451/how-to-save-a-figure-remotely-with-pylab
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


Y_LIM = (0, 20)
DEFAULT_COLORS = plt.rcParams['axes.prop_cycle'].by_key()['color']
COLORS = {
	"nocompression": DEFAULT_COLORS[0],
	"default": DEFAULT_COLORS[1],
	"wc": DEFAULT_COLORS[2]
}


def plot_barchart(x_ticks, series_list, series_labels, series_colors,
				  x_label, y_label,
				  out_file, out_file_format,
				  title,
				  y_lim=None):
	n_groups = len(x_ticks)
	figsize = max(8, n_groups / 6)

	fig, ax = plt.subplots()

	plt.figure(figsize=(2*figsize, figsize), dpi=100)

	index = np.arange(n_groups)
	bar_width = 0.3

	for idx, (s, l,c ) in enumerate(zip(series_list, series_labels, series_colors)):
		rects = plt.bar(index + (idx * bar_width), s, bar_width, label=l, color=c)

	if y_lim is not None:
		plt.ylim(y_lim)

	plt.xlabel(x_label)
	plt.ylabel(y_label)
	plt.xticks(index + ((len(series_list)-1) * bar_width), x_ticks, rotation=270)
	plt.legend()
	plt.title(title)

	plt.tight_layout()

	plt.savefig(out_file, format=out_file_format)


def plot(data, out_dir, out_file_format="svg"):
	data_items = sorted(data.items(), key=lambda x: x[0])

	def to_gib(b):
		return float(b) / 1024 / 1024 / 1024

	# all columns
	table_series = []
	size_nocompression_series = []
	size_default_series, size_wc_series = [], []
	ratio_default_series, ratio_wc_series = [], []
	for (wc, table), summary in data_items:
		table_series.append(table)
		size_nocompression_series.append(to_gib(summary["nocompression_default"]["total"]["size_baseline_B"]))
		size_default_series.append(to_gib(summary["nocompression_default"]["total"]["size_target_B"]))
		size_wc_series.append(to_gib(summary["nocompression_wc"]["total"]["size_target_B"]))
		ratio_default_series.append(summary["nocompression_default"]["total"]["compression_ratio"])
		ratio_wc_series.append(summary["nocompression_wc"]["total"]["compression_ratio"])

	# ratio
	out_file = os.path.join(out_dir, "ratio_total.{}".format(out_file_format))
	plot_barchart(table_series,
				  [ratio_default_series, ratio_wc_series],
				  ["baseline default", "whitebox compression"],
				  [COLORS["default"], COLORS["wc"]],
				  "table", "compression ratio",
				  out_file, out_file_format,
				  title="Compression ratio")

	# sizes
	out_file = os.path.join(out_dir, "size_total.{}".format(out_file_format))
	plot_barchart(table_series,
				  [size_nocompression_series, size_default_series, size_wc_series],
				  ["baseline no compression", "baseline default", "whitebox compression"],
				  [COLORS["nocompression"], COLORS["default"], COLORS["wc"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Total table size")

	# used columns
	table_series = []
	size_default_series, size_wc_series = [], []
	for (wc, table), summary in data_items:
		# NOTE: filter cases where VectorWise put multiple columns in the same file
		if "used" not in summary["default_wc"]:
			continue
		table_series.append(table)
		size_default_series.append(to_gib(summary["default_wc"]["used"]["size_baseline_B"]))
		size_wc_series.append(to_gib(summary["default_wc"]["used"]["size_target_B"]))

	# used size
	out_file = os.path.join(out_dir, "size_used.{}".format(out_file_format))
	plot_barchart(table_series,
				  [size_default_series, size_wc_series],
				  ["baseline default", "whitebox compression"],
				  [COLORS["default"], COLORS["wc"]],
				  "table", "total size of columns present in the expression tree (GiB)",
				  out_file, out_file_format,
				  title="Used columns size")


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Plot evaluation comparison results."""
	)

	parser.add_argument('--wbs-dir', dest='wbs_dir', type=str,
		help="Path to workbooks directory")
	parser.add_argument('--testset-dir', dest='testset_dir', type=str,
		help="Path to testset directory")
	parser.add_argument('--out-dir', dest='out_dir', type=str,
		help="Output directory to save plots to")
	parser.add_argument('--out-file-format', dest='out_file_format', type=str,
		help="Format of the ouput files", default="svg")

	return parser.parse_args()


def main_helper(wbs_dir, testset_dir, out_dir, out_file_format, base_dir_extension):
	data = {}

	for wb in os.listdir(testset_dir):
		with open(os.path.join(testset_dir, wb), 'r') as fp_wb:
			for table in fp_wb:
				table = table.strip()

				base_dir = os.path.join(wbs_dir, wb,
										"{}.{}".format(table, base_dir_extension),
										"compare_stats")
				summary_out_file_nocompression_default = os.path.join(base_dir, "{}.summary.nocompression-default.json".format(table))
				summary_out_file_nocompression_wc = os.path.join(base_dir, "{}.summary.nocompression-wc.json".format(table))
				summary_out_file_default_wc = os.path.join(base_dir, "{}.summary.default-wc.json".format(table))

				try:
					with open(summary_out_file_nocompression_default, 'r') as fp_nocompression_default, \
						 open(summary_out_file_nocompression_wc, 'r') as fp_nocompression_wc, \
						 open(summary_out_file_default_wc, 'r') as fp_default_wc:
						data[(wb, table)] = {
							"nocompression_default": json.load(fp_nocompression_default),
							"nocompression_wc": json.load(fp_nocompression_wc),
							"default_wc": json.load(fp_default_wc),
						}
				except Exception as e:
					print('error: unable to load data for ({}, {}): error={}'.format(wb, table, e))

	plot(data, out_dir=out_dir, out_file_format=out_file_format)


def main(wbs_dir, testset_dir, out_dir, out_file_format):

	# vectorwise baseline
	out_dir_tmp = os.path.join(out_dir, "vectorwise")
	if not os.path.exists(out_dir_tmp):
		os.mkdir(out_dir_tmp)
	main_helper(wbs_dir, testset_dir, out_dir_tmp, out_file_format,
				base_dir_extension="poc_1_out")

	# theoretical baseline
	out_dir_tmp = os.path.join(out_dir, "theoretical")
	if not os.path.exists(out_dir_tmp):
		os.mkdir(out_dir_tmp)
	main_helper(wbs_dir, testset_dir, out_dir_tmp, out_file_format,
				base_dir_extension="poc_1_out-theoretical")


if __name__ == "__main__":
	args = parse_args()
	print(args)

	main(args.wbs_dir, args.testset_dir, args.out_dir, args.out_file_format)
