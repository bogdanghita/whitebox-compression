#!/usr/bin/env python3

import os, sys
import argparse
import numpy as np
import json
from copy import copy, deepcopy

# NOTE: this is needed when running on remote server through ssh
# see: https://stackoverflow.com/questions/4706451/how-to-save-a-figure-remotely-with-pylab
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


Y_LIM_size, Y_LIM_ratio = (0, 15), (0, 28)
DEFAULT_COLORS = plt.rcParams['axes.prop_cycle'].by_key()['color']
COLORS = {
	"nocompression": DEFAULT_COLORS[0],
	"default": DEFAULT_COLORS[1],
	"wc": DEFAULT_COLORS[2],
	"total": DEFAULT_COLORS[3],
	"used": DEFAULT_COLORS[4],
	"theoretical": DEFAULT_COLORS[5],
	"vectorwise": DEFAULT_COLORS[6],
}


def to_gib(b):
	return float(b) / 1024 / 1024 / 1024


def reorder(values, order):
	values_tmp = copy(values)
	for i, j in enumerate(order):
		values[i] = values_tmp[j]


def plot_barchart(x_ticks, series_list, series_labels, series_colors,
				  x_label, y_label,
				  out_file, out_file_format,
				  title,
				  order=None,
				  y_lim=None):
	x_ticks = deepcopy(x_ticks)
	series_list = deepcopy(series_list)
	if order is not None:
		for v_list in series_list + [x_ticks]:
			reorder(v_list, order)

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


def plot_total(data_items, out_dir, out_file_format):
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
	order_ratio = sorted(range(len(data_items)), 
				   key=lambda i: data_items[i][1]["nocompression_wc"]["total"]["compression_ratio"],
				   reverse=True)
	out_file = os.path.join(out_dir, "ratio_total.{}".format(out_file_format))
	plot_barchart(table_series,
				  [ratio_default_series, ratio_wc_series],
				  ["blackbox compression", "whitebox compression"],
				  [COLORS["default"], COLORS["wc"]],
				  "table", "compression ratio",
				  out_file, out_file_format,
				  title="Compression ratio",
				  order=order_ratio,
				  y_lim=Y_LIM_ratio)

	# size
	order_size = order_ratio
	out_file = os.path.join(out_dir, "size_total.{}".format(out_file_format))
	plot_barchart(table_series,
				  [size_nocompression_series, size_default_series, size_wc_series],
				  ["no compression", "blackbox compression", "whitebox compression"],
				  [COLORS["nocompression"], COLORS["default"], COLORS["wc"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Total table size",
				  order=order_size,
				  y_lim=Y_LIM_size)

	return {
		"table_series": table_series,
		"size_nocompression_series": size_nocompression_series,
		"size_default_series": size_default_series,
		"size_wc_series": size_wc_series,
		"ratio_default_series": ratio_default_series,
		"ratio_wc_series": ratio_wc_series,
		"order_ratio": order_ratio,
		"order_size": order_size
	}


def plot_used(data_items, out_dir, out_file_format):
	table_series = []
	size_nocompression_series = []
	size_default_series, size_wc_series = [], []
	ratio_default_series, ratio_wc_series = [], []
	for (wc, table), summary in data_items:
		# NOTE: filter cases where VectorWise put multiple columns in the same file
		if "used" not in summary["default_wc"]:
			print("debug: \"used\" not in summary[\"default_wc\"]; wc={}, table={}".format(wc, table))
			continue
		size_nocompression = summary["nocompression_wc"]["used"]["size_baseline_B"]
		size_default = summary["default_wc"]["used"]["size_baseline_B"]
		size_wc = summary["default_wc"]["used"]["size_target_B"]

		table_series.append(table)
		size_nocompression_series.append(to_gib(size_nocompression))
		size_default_series.append(to_gib(size_default))
		size_wc_series.append(to_gib(size_wc))
		ratio_default_series.append(float(size_nocompression) / size_default)
		ratio_wc_series.append(float(size_nocompression) / size_wc)

	# ratio
	order_ratio = sorted(range(len(table_series)), 
				   key=lambda i: ratio_wc_series[i],
				   reverse=True)
	out_file = os.path.join(out_dir, "ratio_used.{}".format(out_file_format))
	plot_barchart(table_series,
				  [ratio_default_series, ratio_wc_series],
				  ["blackbox compression", "whitebox compression"],
				  [COLORS["default"], COLORS["wc"]],
				  "table", "compression ratio",
				  out_file, out_file_format,
				  title="Used columns ratio",
				  order=order_ratio,
				  y_lim=Y_LIM_ratio)

	# size
	order_size = order_ratio
	out_file = os.path.join(out_dir, "size_used.{}".format(out_file_format))
	plot_barchart(table_series,
				  [size_nocompression_series, size_default_series, size_wc_series],
				  ["no compression", "blackbox compression", "whitebox compression"],
				  [COLORS["nocompression"], COLORS["default"], COLORS["wc"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Used columns size",
				  order=order_size,
				  y_lim=Y_LIM_size)

	return {
		"table_series": table_series,
		"size_nocompression_series": size_nocompression_series,
		"size_default_series": size_default_series,
		"size_wc_series": size_wc_series,
		"ratio_default_series": ratio_default_series,
		"ratio_wc_series": ratio_wc_series,
		"order_ratio": order_ratio,
		"order_size": order_size
	}


def plot_total_vs_used(series_total, series_used, out_dir, out_file_format, order=None):
	total_table_series = series_total["table_series"]
	used_table_series = series_used["table_series"]
	total_size_series = []
	used_size_series = []

	i, j = 0, 0
	while j < len(used_table_series):
		if total_table_series[i] != used_table_series[j]:
			i += 1
			continue
		total_size_series.append(series_total["size_nocompression_series"][i])
		used_size_series.append(series_used["size_nocompression_series"][j])
		i += 1
		j += 1

	out_file = os.path.join(out_dir, "size_total_vs_used.{}".format(out_file_format))
	plot_barchart(used_table_series,
				  [total_size_series, used_size_series],
				  ["total size", "used size"],
				  [COLORS["total"], COLORS["used"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Total vs used columns size",
				  order=order,
				  y_lim=Y_LIM_size)


def plot_baseline_helper(data, out_dir, out_file_format):
	data_items = sorted(data.items(), key=lambda x: x[0])

	# total
	series_total = plot_total(data_items, out_dir, out_file_format)

	# used
	series_used = plot_used(data_items, out_dir, out_file_format)

	# total vs used size
	order = series_used["order_ratio"]
	plot_total_vs_used(series_total, series_used, out_dir, out_file_format, order=order)

	return (series_total, series_used)


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


def plot_baseline(wbs_dir, testset_dir, out_dir, out_file_format, base_dir_extension):
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

	return plot_baseline_helper(data, out_dir, out_file_format)


def plot_comparison(series_vectorwise, series_theoretical, out_dir, out_file_format):
	series_vectorwise_orig = series_vectorwise
	series_theoretical_orig = series_theoretical

	table_series = series_vectorwise[0]["table_series"]

	# no-compression
	series_vectorwise  = series_vectorwise_orig[0]["size_nocompression_series"]
	series_theoretical = series_theoretical_orig[0]["size_nocompression_series"]
	out_file = os.path.join(out_dir, "no_compression.{}".format(out_file_format))
	plot_barchart(table_series,
				  [series_theoretical, series_vectorwise],
				  ["theoretical", "vectorwise"],
				  [COLORS["theoretical"], COLORS["vectorwise"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Theoretical vs VectorWise (no compression)")

	# default
	series_vectorwise  = series_vectorwise_orig[0]["size_default_series"]
	series_theoretical = series_theoretical_orig[0]["size_default_series"]
	out_file = os.path.join(out_dir, "blackbox_compression.{}".format(out_file_format))
	plot_barchart(table_series,
				  [series_theoretical, series_vectorwise],
				  ["theoretical", "vectorwise"],
				  [COLORS["theoretical"], COLORS["vectorwise"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Theoretical vs VectorWise (blackbox compression)")

	# wc
	series_vectorwise  = series_vectorwise_orig[0]["size_wc_series"]
	series_theoretical = series_theoretical_orig[0]["size_wc_series"]
	out_file = os.path.join(out_dir, "whitebox_compression.{}".format(out_file_format))
	plot_barchart(table_series,
				  [series_theoretical, series_vectorwise],
				  ["theoretical", "vectorwise"],
				  [COLORS["theoretical"], COLORS["vectorwise"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Theoretical vs VectorWise (whitebox compression)")


def main(wbs_dir, testset_dir, out_dir, out_file_format):

	# vectorwise baseline
	out_dir_tmp = os.path.join(out_dir, "vectorwise")
	if not os.path.exists(out_dir_tmp):
		os.mkdir(out_dir_tmp)
	series_vectorwise = plot_baseline(wbs_dir, testset_dir, out_dir_tmp, out_file_format,
									  base_dir_extension="poc_1_out")

	# theoretical baseline
	out_dir_tmp = os.path.join(out_dir, "theoretical")
	if not os.path.exists(out_dir_tmp):
		os.mkdir(out_dir_tmp)
	series_theoretical = plot_baseline(wbs_dir, testset_dir, out_dir_tmp, out_file_format,
									   base_dir_extension="poc_1_out-theoretical")

	# theoretical vs vectorwise
	out_dir_tmp = os.path.join(out_dir, "theoretical_vs_vectorwise")
	if not os.path.exists(out_dir_tmp):
		os.mkdir(out_dir_tmp)
	plot_comparison(series_vectorwise, series_theoretical, out_dir_tmp, out_file_format)


if __name__ == "__main__":
	args = parse_args()
	print(args)

	out_file_format = "svg" if args.out_file_format is None else args.out_file_format
	main(args.wbs_dir, args.testset_dir, args.out_dir, out_file_format)
