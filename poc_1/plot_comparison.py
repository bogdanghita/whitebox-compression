#!/usr/bin/env python3

import os, sys
import argparse
import numpy as np
import json
from copy import copy, deepcopy
from statistics import mean
from collections import Counter
from lib.util import *
from pattern_detection.patterns import *
from pattern_detection.lib.expression_tree import *

# NOTE: this is needed when running on remote server through ssh
# see: https://stackoverflow.com/questions/4706451/how-to-save-a-figure-remotely-with-pylab
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

FONT_SIZE_BAR = 8
FONT_SIZE_PIE = 14
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


def sizeof_fmt(num, suffix='B'):
	for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
		if abs(num) < 1024.0:
			return "%.1f%s%s" % (num, unit, suffix)
		num /= 1024.0
	return "%.1f%s%s" % (num, 'Yi', suffix)


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

	plt.rcParams.update({'font.size': FONT_SIZE_BAR})

	fig, ax = plt.subplots()

	# plt.figure(figsize=(2*figsize, figsize), dpi=100)
	plt.figure()

	index = np.arange(n_groups)
	bar_width = 0.3

	for idx, (s, l, c) in enumerate(zip(series_list, series_labels, series_colors)):
		rects = plt.bar(index + (idx * bar_width), s, bar_width, label=l, color=c)

	plt.xlim((-1, len(x_ticks)))
	if y_lim is not None:
		plt.ylim(y_lim)

	plt.xlabel(x_label)
	plt.ylabel(y_label)
	plt.xticks(index + ((len(series_list)-1) * bar_width), x_ticks, rotation=270)
	plt.legend()
	plt.title(title)

	target_aspect_ratio = 0.25
	x_min, x_max = plt.gca().get_xlim()
	y_min, y_max = plt.gca().get_ylim()
	aspect_ratio = target_aspect_ratio / (float(y_max - y_min) / (x_max - x_min))
	# print(x_min, x_max, y_min, y_max, aspect_ratio)
	plt.axes().set_aspect(aspect=aspect_ratio)
	plt.tight_layout()

	plt.savefig(out_file, bbox_inches='tight', format=out_file_format)


def plot_piechart(values, labels,
			  	  out_file, out_file_format,
			  	  colors=None,
			  	  title=None):
	plt.rcParams.update({'font.size': FONT_SIZE_PIE})

	fig1, ax1 = plt.subplots()
	patches, texts, autotexts = ax1.pie(values, labels=labels,
			colors=colors,
			# explode=explode,
			autopct='%1.1f%%',
			# startangle=90,
			radius=0.8,
			pctdistance=0.8, # default: 0.6
			labeldistance=1.1, # default: 1
			)
	# for text in texts:
	# 	text.set_color('grey')
	for autotext in autotexts:
		autotext.set_color('white')

	centre_circle = plt.Circle((0,0),0.45,fc='white')
	fig = plt.gcf()
	fig.gca().add_artist(centre_circle)

	ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

	# plt.legend(patches, labels, loc='left center', bbox_to_anchor=(-0.1, 1.))

	if title is not None:
		plt.title(title)
	plt.tight_layout()

	plt.savefig(out_file, format=out_file_format)


def plot_barchart_multiple(x_ticks, x_label,
						   plot_data_list,
						   out_file, out_file_format,
						   order=None):
	x_ticks = deepcopy(x_ticks)
	if order is not None:
		reorder(x_ticks, order)

	n_groups = len(x_ticks)
	figsize = max(8, n_groups / 6)

	plt.rcParams.update({'font.size': FONT_SIZE_BAR})

	plt.figure()

	for i, plot_data in enumerate(plot_data_list):
		series_list = plot_data["series_list"]
		series_labels = plot_data["series_labels"]
		series_colors = plot_data["series_colors"]
		y_label = plot_data["y_label"]
		y_lim = plot_data["y_lim"] if "y_lim" in plot_data else None
		title = plot_data["title"] if "title" in plot_data else None

		series_list = deepcopy(series_list)
		if order is not None:
			for v_list in series_list:
				reorder(v_list, order)

		index = np.arange(n_groups)
		bar_width = 0.3

		plt.subplot(len(plot_data_list), 1, i+1)

		for idx, (s, l, c) in enumerate(zip(series_list, series_labels, series_colors)):
			rects = plt.bar(index + (idx * bar_width), s, bar_width, label=l, color=c)

		plt.xticks(index + ((len(series_list)-1) * bar_width), [])

		plt.xlim((-1, len(x_ticks)))
		if y_lim is not None:
			plt.ylim(y_lim)

		if title is not None:
			plt.title(title)

		plt.ylabel(y_label)
		plt.legend()

	plt.xlabel(x_label)
	plt.xticks(index + ((len(series_list)-1) * bar_width), x_ticks, rotation=270)

	plt.tight_layout()
	plt.savefig(out_file, bbox_inches='tight', format=out_file_format)


def plot_total(data_items, out_dir, out_file_format,
			   baseline=None):
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

	if baseline == "Estimator model":
		blackbox_label = "basic lightweight"
	else:
		blackbox_label = "blackbox"

	# ratio
	order_ratio = sorted(range(len(data_items)), 
				   key=lambda i: data_items[i][1]["nocompression_wc"]["total"]["compression_ratio"],
				   reverse=True)
	out_file = os.path.join(out_dir, "ratio_total.{}".format(out_file_format))
	plot_barchart(table_series,
				  [ratio_default_series, ratio_wc_series],
				  ["{} compression".format(blackbox_label), "whitebox compression"],
				  [COLORS["default"], COLORS["wc"]],
				  "table", "compression ratio",
				  out_file, out_file_format,
				  title="Compression ratio ({} baseline)".format(baseline),
				  order=order_ratio,
				  # y_lim=Y_LIM_ratio
				  )

	# size
	order_size = order_ratio
	out_file = os.path.join(out_dir, "size_total.{}".format(out_file_format))
	plot_barchart(table_series,
				  [size_nocompression_series, size_default_series, size_wc_series],
				  ["no compression", "{} compression".format(blackbox_label), "whitebox compression"],
				  [COLORS["nocompression"], COLORS["default"], COLORS["wc"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Total table size ({} baseline)".format(baseline),
				  order=order_size,
				  # y_lim=Y_LIM_size
				  )

	# combined
	title = "Full table ({} baseline)".format(baseline)
	plot_data_list = [
		{
			"series_list": [ratio_default_series, ratio_wc_series],
			"series_labels": ["{} compression".format(blackbox_label), "whitebox compression"],
			"series_colors": [COLORS["default"], COLORS["wc"]],
			"y_label": "compression ratio",
			"y_lim": None,
			"title": title
		},
		{
			"series_list": [size_nocompression_series, size_default_series, size_wc_series],
			"series_labels": ["no compression", "{} compression".format(blackbox_label), "whitebox compression"],
			"series_colors": [COLORS["nocompression"], COLORS["default"], COLORS["wc"]],
			"y_label": "table size (GiB)",
			"y_lim": None,
			"title": None
		}
	]
	x_ticks = table_series
	x_label = "table"
	out_file = os.path.join(out_dir, "total.{}".format(out_file_format))
	plot_barchart_multiple(x_ticks, x_label,
						   plot_data_list,
						   out_file, out_file_format,
						   order=order_ratio)

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


def plot_used(data_items, out_dir, out_file_format,
			  baseline=None):
	table_series = []
	size_nocompression_series = []
	size_default_series, size_wc_series = [], []
	ratio_default_series, ratio_wc_series = [], []
	for (wc, table), summary in data_items:
		# NOTE: filter cases where VectorWise put multiple columns in the same file
		if "used" not in summary["default_wc"]:
			print("debug: [plot_used] \"used\" not in summary[\"default_wc\"]; wc={}, table={}, baseline={}".format(wc, table, baseline))
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

	if baseline == "Estimator model":
		blackbox_label = "basic lightweight"
	else:
		blackbox_label = "blackbox"

	# ratio
	order_ratio = sorted(range(len(table_series)), 
				   key=lambda i: ratio_wc_series[i],
				   reverse=True)
	out_file = os.path.join(out_dir, "ratio_used.{}".format(out_file_format))
	plot_barchart(table_series,
				  [ratio_default_series, ratio_wc_series],
				  ["{} compression".format(blackbox_label), "whitebox compression"],
				  [COLORS["default"], COLORS["wc"]],
				  "table", "compression ratio",
				  out_file, out_file_format,
				  title="Used columns ratio ({} baseline)".format(baseline),
				  order=order_ratio,
				  # y_lim=Y_LIM_ratio
				  )

	# size
	order_size = order_ratio
	out_file = os.path.join(out_dir, "size_used.{}".format(out_file_format))
	plot_barchart(table_series,
				  [size_nocompression_series, size_default_series, size_wc_series],
				  ["no compression", "{} compression".format(blackbox_label), "whitebox compression"],
				  [COLORS["nocompression"], COLORS["default"], COLORS["wc"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Used columns size ({} baseline)".format(baseline),
				  order=order_size,
				  # y_lim=Y_LIM_size
				  )

	# combined
	title = "Used columns ({} baseline)".format(baseline)
	plot_data_list = [
		{
			"series_list": [ratio_default_series, ratio_wc_series],
			"series_labels": ["{} compression".format(blackbox_label), "whitebox compression"],
			"series_colors": [COLORS["default"], COLORS["wc"]],
			"y_label": "compression ratio",
			"y_lim": None,
			"title": title
		},
		{
			"series_list": [size_nocompression_series, size_default_series, size_wc_series],
			"series_labels": ["no compression", "{} compression".format(blackbox_label), "whitebox compression"],
			"series_colors": [COLORS["nocompression"], COLORS["default"], COLORS["wc"]],
			"y_label": "table size (GiB)",
			"y_lim": None,
			"title": None
		}
	]
	x_ticks = table_series
	x_label = "table"
	out_file = os.path.join(out_dir, "used.{}".format(out_file_format))
	plot_barchart_multiple(x_ticks, x_label,
						   plot_data_list,
						   out_file, out_file_format,
						   order=order_ratio)

	return {
		"table_series": table_series,
		"size_nocompression_series": size_nocompression_series,
		"size_default_series": size_default_series,
		"size_wc_series": size_wc_series,
		"ratio_default_series": ratio_default_series,
		"ratio_wc_series": ratio_wc_series,
		"order_ratio": order_ratio,
		"order_size": order_size,
	}


def plot_total_vs_used(series_total, series_used, out_dir, out_file_format, 
					   order=None, baseline=None):
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
				  title="Total vs used columns size ({} baseline)".format(baseline),
				  order=order,
				  # y_lim=Y_LIM_size
				  )


def plot_baseline_helper(data, out_dir, out_file_format, baseline):
	data_items = sorted(data.items(), key=lambda x: x[0])

	# total
	series_total = plot_total(data_items, out_dir, out_file_format, baseline)

	# used
	series_used = plot_used(data_items, out_dir, out_file_format, baseline)

	# total vs used size
	order = series_used["order_ratio"]
	plot_total_vs_used(series_total, series_used, out_dir, out_file_format, order=order, baseline=baseline)

	return (series_total, series_used)


def plot_column_stats(data_items, out_dir, out_file_format, baseline):
	table_count = 0
	in_columns_count_total, in_columns_size_total = 0, 0
	used_columns_count_total = 0
	out_columns_count_total, out_columns_size_total = 0, 0
	ex_columns_count_total, ex_columns_size_total = 0, 0
	metadata_size_total = 0
	in_datatypes_total, out_datatypes_total, ex_datatypes_total = Counter(), Counter(), Counter()

	for (wc, table), summary in data_items:
		# NOTE: filter cases where VectorWise put multiple columns in the same file
		if "used" not in summary["default_wc"]:
			print("debug: [plot_column_stats] \"used\" not in summary[\"default_wc\"]; wc={}, table={}, baseline={}".format(wc, table, baseline))
			continue
		summary = summary["nocompression_wc"]
		table_count += 1
		in_columns_count_total += sum(summary["in_datatypes"].values())
		in_columns_size_total += summary["used"]["size_baseline_B"]
		used_columns_count_total += sum(summary["used"]["datatypes"]["in_columns"].values())
		out_columns_count_total += sum(summary["used"]["datatypes"]["out_columns"].values())
		out_columns_size_total += summary["used"]["size_components"]["out_size_B"]
		ex_columns_count_total += sum(summary["used"]["datatypes"]["ex_columns"].values())
		ex_columns_size_total += summary["used"]["size_components"]["ex_size_B"]
		metadata_size_total += summary["used"]["size_components"]["metadata_size_B"]
		in_datatypes_total += summary["used"]["datatypes"]["in_columns"]
		out_datatypes_total += summary["used"]["datatypes"]["out_columns"]
		ex_datatypes_total += summary["used"]["datatypes"]["ex_columns"]

	datatypes_total = in_datatypes_total + out_datatypes_total + ex_datatypes_total
	datatype_colors = {}
	for idx, (datatype, count) in enumerate(datatypes_total.most_common()):
		# print(datatype, idx)
		datatype_colors[datatype] = DEFAULT_COLORS[idx]

	in_columns_count_avg = float(in_columns_count_total) / table_count
	in_columns_size_avg = float(in_columns_size_total) / table_count
	used_columns_count_avg = float(used_columns_count_total) / table_count
	out_columns_count_avg = float(out_columns_count_total) / table_count
	out_columns_size_avg = float(out_columns_size_total) / table_count
	ex_columns_count_avg = float(ex_columns_count_total) / table_count
	ex_columns_size_avg = float(ex_columns_size_total) / table_count
	metadata_size_avg = float(metadata_size_total) / table_count
	in_datatypes_avg = {k: float(v) / table_count for (k, v) in in_datatypes_total.items()}
	out_datatypes_avg = {k: float(v) / table_count for (k, v) in out_datatypes_total.items()}
	ex_datatypes_avg = {k: float(v) / table_count for (k, v) in ex_datatypes_total.items()}

	print("table_count={}".format(table_count))
	print("in_columns_count_total={}\nin_columns_size_total={}\nused_columns_count_total={}\nout_columns_count_total={}\nout_columns_size_total={}\nex_columns_count_total={}\nex_columns_size_total={}\nmetadata_size_total={}\nin_datatypes_total={}\nout_datatypes_total={}\nex_datatypes_total={}".format(in_columns_count_total,sizeof_fmt(in_columns_size_total),used_columns_count_total,out_columns_count_total,sizeof_fmt(out_columns_size_total),ex_columns_count_total,sizeof_fmt(ex_columns_size_total),sizeof_fmt(metadata_size_total),in_datatypes_total,out_datatypes_total,ex_datatypes_total))
	print("in_columns_count_avg={}\nin_columns_size_avg={}\nused_columns_count_avg={}\nout_columns_count_avg={}\nout_columns_size_avg={}\nex_columns_count_avg={}\nex_columns_size_avg={}\nmetadata_size_avg={}\nin_datatypes_avg={}\nout_datatypes_avg={}\nex_datatypes_avg={}".format(in_columns_count_avg,sizeof_fmt(in_columns_size_avg),used_columns_count_avg,out_columns_count_avg,sizeof_fmt(out_columns_size_avg),ex_columns_count_avg,sizeof_fmt(ex_columns_size_avg),sizeof_fmt(metadata_size_avg),in_datatypes_avg,out_datatypes_avg,ex_datatypes_avg))

	# used columns datatype distribution
	labels = in_datatypes_avg.keys()
	values = [in_datatypes_avg[k] for k in labels]
	try:
		colors = [datatype_colors[k] for k in labels]
	except Exception as e:
		print("debug: unable to find color for datatype; falling back to default colors")
		colors = None
	out_file = os.path.join(out_dir, "used_datatypes.{}".format(out_file_format))
	title = "Used columns datatype distribution"
	plot_piechart(values, labels,
				  out_file, out_file_format,
				  colors=colors,
				  title=None)

	# out columns datatype distribution
	labels = out_datatypes_avg.keys()
	values = [out_datatypes_avg[k] for k in labels]
	try:
		colors = [datatype_colors[k] for k in labels]
	except Exception as e:
		print("debug: unable to find color for datatype; falling back to default colors")
		colors = None
	out_file = os.path.join(out_dir, "out_datatypes.{}".format(out_file_format))
	title = "Ouput columns datatype distribution"
	plot_piechart(values, labels,
				  out_file, out_file_format,
				  colors=colors,
				  title=None)

	# out size distribution
	labels = ["metadata", "data", "exceptions"]
	colors = [DEFAULT_COLORS[0], DEFAULT_COLORS[2], DEFAULT_COLORS[1]]
	values = [metadata_size_avg, out_columns_size_avg, ex_columns_size_avg]
	out_file = os.path.join(out_dir, "out_size_distribution.{}".format(out_file_format))
	title = "Ouput size distribution"
	plot_piechart(values, labels,
				  out_file, out_file_format,
				  colors=colors,
				  title=None)


def plot_expression_tree_stats(data_items, out_dir, out_file_format, baseline):
	label_map = {
		"ColumnCorrelation": "correlation",
		"CharSetSplit": "split",
		"NumberAsString": "numeric\nstrings",
		"ConstantPatternDetector": "constant",
		"DictPattern": "dictionary"
	}

	table_count, cc_count = 0, 0
	expr_n_types_total = Counter()
	cc_depth_total, cc_expr_n_total, cc_in_columns_total, cc_out_columns_total = 0, 0, 0, 0

	for (wc, table), summary in data_items:
		# NOTE: filter cases where VectorWise put multiple columns in the same file
		if "used" not in summary["default_wc"]:
			print("debug: [plot_expression_tree_stats] \"used\" not in summary[\"default_wc\"]; wc={}, table={}, baseline={}".format(wc, table, baseline))
			continue
		summary = summary["nocompression_wc"]
		table_count += 1

		# ccs
		for cc_idx in summary["used"]["expr_tree"]["cc_depths"]:
			# if summary["used"]["expr_tree"]["cc_nb_expr_nodes"][cc_idx] == 1:
			# 	continue
			cc_count += 1
			cc_depth_total += summary["used"]["expr_tree"]["cc_depths"][cc_idx]
			cc_expr_n_total += summary["used"]["expr_tree"]["cc_nb_expr_nodes"][cc_idx]
			cc_in_columns_total += summary["used"]["expr_tree"]["cc_in_columns"][cc_idx]
			if cc_idx in summary["used"]["expr_tree"]["cc_out_columns"]:
				cc_out_columns_total += summary["used"]["expr_tree"]["cc_out_columns"][cc_idx]

		# pattern types
		for cc_idx, patterns in summary["used"]["expr_tree"]["cc_patterns"].items():
			expr_n_types_total += patterns

	table_cc_avg = float(cc_count) / table_count
	cc_depth_avg = float(cc_depth_total) / cc_count
	cc_expr_n_avg = float(cc_expr_n_total) / cc_count
	cc_in_columns_avg = float(cc_in_columns_total) / cc_count
	cc_out_columns_avg = float(cc_out_columns_total) / cc_count

	expr_n_types_avg = {k: float(v) / table_count for (k, v) in expr_n_types_total.items()}

	print("table_cc_avg={}\ncc_depth_avg={}\ncc_expr_n_avg={}\ncc_in_columns_avg={}\ncc_out_columns_avg={}".format(table_cc_avg,cc_depth_avg,cc_expr_n_avg,cc_in_columns_avg,cc_out_columns_avg))

	# expression node types distribution
	patterns = expr_n_types_avg.keys()
	values = [expr_n_types_avg[k] for k in patterns]
	labels = [label_map[k] for k in patterns]
	out_file = os.path.join(out_dir, "expr_n_types.{}".format(out_file_format))
	title = "Expression node types distribution"
	plot_piechart(values, labels,
				  out_file, out_file_format,
				  title=None)


def plot_stats(data, out_dir, out_file_format, baseline):
	data_items = sorted(data.items(), key=lambda x: x[0])

	plot_column_stats(data_items, out_dir, out_file_format, baseline)
	plot_expression_tree_stats(data_items, out_dir, out_file_format, baseline)


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Plot evaluation comparison results."""
	)

	parser.add_argument('--wbs-dir', dest='wbs_dir', type=str,
		help="Path to workbooks directory")
	parser.add_argument('--repo-wbs-dir', dest='repo_wbs_dir', type=str,
		help="Path to PBIB benchmark directory")
	parser.add_argument('--testset-dir', dest='testset_dir', type=str,
		help="Path to testset directory")
	parser.add_argument('--out-dir', dest='out_dir', type=str,
		help="Output directory to save plots to")
	parser.add_argument('--out-file-format', dest='out_file_format', type=str,
		help="Format of the ouput files", default="svg")

	return parser.parse_args()


def plot_baseline(wbs_dir, testset_dir, out_dir, out_file_format, base_dir_extension, 
				  baseline="default"):
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

	# plot other stats
	plot_stats(data, out_dir, out_file_format, baseline)

	# plot size and ratios
	return plot_baseline_helper(data, out_dir, out_file_format, baseline)


def plot_comparison(series_vectorwise, series_theoretical, out_dir, out_file_format):
	series_vectorwise_orig = series_vectorwise
	series_theoretical_orig = series_theoretical

	table_series = series_vectorwise[0]["table_series"]

	# no-compression
	series_vectorwise_nc  = series_vectorwise_orig[0]["size_nocompression_series"]
	series_theoretical_nc = series_theoretical_orig[0]["size_nocompression_series"]
	order_nc = sorted(range(len(table_series)), 
					  key=lambda i: series_vectorwise_nc[i],
					  reverse=True)
	out_file = os.path.join(out_dir, "no_compression.{}".format(out_file_format))
	plot_barchart(table_series,
				  [series_theoretical_nc, series_vectorwise_nc],
				  ["estimator model", "vectorwise"],
				  [COLORS["theoretical"], COLORS["vectorwise"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="No compression",
				  order=order_nc)

	# default
	series_vectorwise_d  = series_vectorwise_orig[0]["size_default_series"]
	series_theoretical_d = series_theoretical_orig[0]["size_default_series"]
	out_file = os.path.join(out_dir, "blackbox_compression.{}".format(out_file_format))
	plot_barchart(table_series,
				  [series_theoretical_d, series_vectorwise_d],
				  ["estimator model", "vectorwise"],
				  [COLORS["theoretical"], COLORS["vectorwise"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Basic lightweight/Blackbox compression",
				  order=order_nc)

	# wc
	series_vectorwise_wc  = series_vectorwise_orig[0]["size_wc_series"]
	series_theoretical_wc = series_theoretical_orig[0]["size_wc_series"]
	out_file = os.path.join(out_dir, "whitebox_compression.{}".format(out_file_format))
	plot_barchart(table_series,
				  [series_theoretical_wc, series_vectorwise_wc],
				  ["estimator model", "vectorwise"],
				  [COLORS["theoretical"], COLORS["vectorwise"]],
				  "table", "table size (GiB)",
				  out_file, out_file_format,
				  title="Whitebox compression",
				  order=order_nc)

	# combined
	plot_data_list = [
		{
			"series_list": [series_theoretical_nc, series_vectorwise_nc],
			"series_labels": ["estimator model", "vectorwise"],
			"series_colors": [COLORS["theoretical"], COLORS["vectorwise"]],
			"y_label": "table size (GiB)",
			"y_lim": None,
			"title": "No compression"
		},
		{
			"series_list": [series_theoretical_d, series_vectorwise_d],
			"series_labels": ["estimator model", "vectorwise"],
			"series_colors": [COLORS["theoretical"], COLORS["vectorwise"]],
			"y_label": "table size (GiB)",
			"y_lim": None,
			"title": "Basic lightweight/Blackbox compression"
		},
		{
			"series_list": [series_theoretical_wc, series_vectorwise_wc],
			"series_labels": ["estimator model", "vectorwise"],
			"series_colors": [COLORS["theoretical"], COLORS["vectorwise"]],
			"y_label": "table size (GiB)",
			"y_lim": None,
			"title": "Whitebox compression"
		}
	]
	x_ticks = table_series
	x_label = "table"
	out_file = os.path.join(out_dir, "combined.{}".format(out_file_format))
	plot_barchart_multiple(x_ticks, x_label,
						   plot_data_list,
						   out_file, out_file_format,
						   order=order_nc)


def compute_general_stats(series_vectorwise, series_theoretical):
	"""
	series_total, series_used = series_vectorwise
	{
		"table_series": table_series,
		"size_nocompression_series": size_nocompression_series,
		"size_default_series": size_default_series,
		"size_wc_series": size_wc_series,
		"ratio_default_series": ratio_default_series,
		"ratio_wc_series": ratio_wc_series,
		"order_ratio": order_ratio,
		"order_size": order_size,
	}
	"""
	res = {}
	for series, name in [(series_vectorwise, "vectorwise"), (series_theoretical, "theoretical")]:
		stats = {}

		stats["size_avg_nocomp_total"] = mean(series[0]["size_nocompression_series"])
		stats["size_avg_vw_total"] = mean(series[0]["size_default_series"])
		stats["size_avg_wb_total"] = mean(series[0]["size_wc_series"])
		stats["ratio_avg_vw_total"] = mean(series[0]["ratio_default_series"])
		stats["ratio_avg_wb_total"] = mean(series[0]["ratio_wc_series"])

		stats["size_avg_nocomp_used"] = mean(series[1]["size_nocompression_series"])
		stats["size_avg_vw_used"] = mean(series[1]["size_default_series"])
		stats["size_avg_wb_used"] = mean(series[1]["size_wc_series"])
		stats["ratio_avg_vw_used"] = mean(series[1]["ratio_default_series"])
		stats["ratio_avg_wb_used"] = mean(series[1]["ratio_wc_series"])

		stats["size_overall_nocomp_total"] = sum(series[0]["size_nocompression_series"])
		stats["size_overall_default_total"] = sum(series[0]["size_default_series"])
		stats["size_overall_wb_total"] = sum(series[0]["size_wc_series"])
		stats["ratio_overall_vw_total"] = stats["size_overall_nocomp_total"] / stats["size_overall_default_total"]
		stats["ratio_overall_wc_total"] = stats["size_overall_nocomp_total"] / stats["size_overall_wb_total"]

		stats["size_overall_nocomp_used"] = sum(series[1]["size_nocompression_series"])
		stats["size_overall_default_used"] = sum(series[1]["size_default_series"])
		stats["size_overall_wb_used"] = sum(series[1]["size_wc_series"])
		stats["ratio_overall_vw_used"] = stats["size_overall_nocomp_used"] / stats["size_overall_default_used"]
		stats["ratio_overall_wc_used"] = stats["size_overall_nocomp_used"] / stats["size_overall_wb_used"]

		res[name] = stats

	return res


def compute_applyexpr_stats(wbs_dir, repo_wbs_dir, testset_dir):
	total_stats = {
		"exout_col_count": 0,
		"in_attr_count": [],
		"ex_attr_count": [],
		"ex_ratio": []
	}

	statdump_stats_file = os.path.join(repo_wbs_dir,
									   "../scripts/analysis/data_analysis/output/stats.json")
	with open(statdump_stats_file, 'r') as fp:
		statdump_stats = json.load(fp)

	for wb in os.listdir(testset_dir):
		with open(os.path.join(testset_dir, wb), 'r') as fp_wb:
			for table in fp_wb:
				table = table.strip()

				if table not in statdump_stats["tables"]:
					raise Exception("Table not found in statdump file: table={}".format(table))

				exprtree_file = os.path.join(wbs_dir, wb, 
											 "{}.expr_tree/c_tree.json".format(table))
				applyexpr_stats_file = os.path.join(wbs_dir, wb, 
													"{}.poc_1_out/".format(table),
													"{}_out.stats.json".format(table))
				linecount_file = os.path.join(repo_wbs_dir, wb, 
											  "samples", 
											  "{}.linecount".format(table))

				expr_tree = read_expr_tree(exprtree_file)

				try:
					with open(applyexpr_stats_file, 'r') as fp_applyexpr, \
						 open(linecount_file, 'r') as fp_linecount:
						applyexpr_stats = json.load(fp_applyexpr)
						linecount = int(fp_linecount.read())
				except Exception as e:
					print('error: unable to load apply expression stats for ({}, {}): error={}'.format(wb, table, e))
					raise e

				all_columns = {}

				input_columns = {}
				for in_col_id in expr_tree.get_in_columns():
					if in_col_id not in statdump_stats["tables"][table]:
						raise Exception("Input column not found in statdump file: in_col_id={}".format(in_col_id))
					col_statdump_stats = statdump_stats["tables"][table][in_col_id]["statdump"]
					input_columns[in_col_id] = deepcopy(col_statdump_stats)
					all_columns[in_col_id] = {
						"null_count": col_statdump_stats["null_ratio"] * linecount
					}

				innerout_columns = {}
				for level, level_data in applyexpr_stats["level_stats"].items():
					for out_col in level_data["out_columns"]:
						# NOTE: add output columns only for the level they are created on; the other levels just ovewrites the metrics
						if out_col["col_id"] in innerout_columns:
							continue
						innerout_columns[out_col["col_id"]] = out_col
						all_columns[out_col["col_id"]] = {
							"null_count": out_col["null_count"]
						}

				for ex_col_id, ex_col in all_columns.items():
					# we only consider output exception columns
					if not (ex_col_id in expr_tree.get_out_columns() and
							OutputColumnManager.is_exception_col_id(ex_col_id)):
						continue
					in_col_id = OutputColumnManager.get_input_col_id(ex_col_id)
					if in_col_id not in all_columns:
						raise Exception("Input column not found: ex_col_id={}, in_col_id={}".format(ex_col_id, in_col_id))
					in_col = all_columns[in_col_id]
					
					# gather metrics
					in_attr_count = linecount - in_col["null_count"]
					ex_attr_count = linecount - ex_col["null_count"]
					ex_ratio = float(ex_attr_count) / in_attr_count
					total_stats["exout_col_count"] += 1
					total_stats["in_attr_count"].append(in_attr_count)
					total_stats["ex_attr_count"].append(ex_attr_count)
					total_stats["ex_ratio"].append(ex_ratio)

					# print(ex_col_id, in_col_id, ex_col, in_col, in_attr_count, ex_attr_count, ex_ratio)

	total_in_attr_count = sum(total_stats["in_attr_count"])
	total_ex_attr_count = sum(total_stats["ex_attr_count"])
	total_ex_attr_ratio = float(total_ex_attr_count) / total_in_attr_count
	avg_ex_ratio = np.mean(total_stats["ex_ratio"])

	res = {
		"total_in_attr_count": total_in_attr_count,
		"total_ex_attr_count": total_ex_attr_count,
		"total_ex_attr_ratio": total_ex_attr_ratio,
		"avg_ex_ratio": avg_ex_ratio
	}
	return res


def main(wbs_dir, repo_wbs_dir, testset_dir, out_dir, out_file_format):
	
	# vectorwise baseline
	out_dir_tmp = os.path.join(out_dir, "vectorwise")
	if not os.path.exists(out_dir_tmp):
		os.mkdir(out_dir_tmp)
	series_vectorwise = plot_baseline(wbs_dir, testset_dir, out_dir_tmp, out_file_format,
									  base_dir_extension="poc_1_out", 
									  baseline="VectorWise")

	# theoretical baseline
	out_dir_tmp = os.path.join(out_dir, "theoretical")
	if not os.path.exists(out_dir_tmp):
		os.mkdir(out_dir_tmp)
	series_theoretical = plot_baseline(wbs_dir, testset_dir, out_dir_tmp, out_file_format,
									   base_dir_extension="poc_1_out-theoretical",
									   baseline="Estimator model")

	# theoretical vs vectorwise
	out_dir_tmp = os.path.join(out_dir, "theoretical_vs_vectorwise")
	if not os.path.exists(out_dir_tmp):
		os.mkdir(out_dir_tmp)
	plot_comparison(series_vectorwise, series_theoretical, out_dir_tmp, out_file_format)

	# general stats
	general_stats = compute_general_stats(series_vectorwise, series_theoretical)
	print(json.dumps(general_stats, indent=2))
	
	# apply_expression stats
	applyexpr_stats = compute_applyexpr_stats(wbs_dir, repo_wbs_dir, testset_dir)
	print(json.dumps(applyexpr_stats, indent=2))


if __name__ == "__main__":
	args = parse_args()
	print(args)

	out_file_format = "svg" if args.out_file_format is None else args.out_file_format
	main(args.wbs_dir, args.repo_wbs_dir, args.testset_dir, args.out_dir, out_file_format)
