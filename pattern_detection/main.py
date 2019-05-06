#!/usr/bin/env python3

import os
import sys
import argparse
import json
import string
from copy import deepcopy
from lib.util import *
from lib.pattern_selectors import *
from patterns import *
from apply_expression import ExpressionManager
from lib.expression_tree import ExpressionTree
from plot_expression_tree import plot_expression_tree
import plot_pattern_distribution, plot_ngram_freq_masks


RET_ERR = 15

# TODO: make these parameters of the script
MAX_ITERATIONS = 100
MIN_COL_COVERAGE = 0.2


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

		return True

	def get_patterns(self):
		patterns = {}

		for pd in self.pattern_detectors:
			patterns[pd.name] = {
				"name": pd.name,
				"columns": pd.evaluate()
			}

		return (patterns, self.total_tuple_count, self.valid_tuple_count)


class OutputManager(object):
	@staticmethod
	def output_stats(columns, patterns):
		for c in columns:
			print(c)
		# print(json.dumps(patterns, indent=2))
		for pd in patterns.values():
			print("*** {} ***".format(pd["name"]))
			for col_id, col_p_list in pd["columns"].items():
				col = next(c for c in columns if c.col_id == col_id)
				print("{}".format(col))
				for p in sorted(col_p_list, key=lambda x: x["coverage"], reverse=True):
					print("{:.2f}\t{}, res_columns={}, ex_columns={}, operator_info={}".format(p["coverage"], p["p_id"], p["res_columns"], p["ex_columns"], p["operator_info"]))

	@staticmethod
	def output_pattern_distribution(level, columns, patterns, pattern_distribution_output_dir, fdelim=",", plot_file_format="svg"):
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

			out_file = "{}/l_{}_col_{}.csv".format(pattern_distribution_output_dir, level, col_id)
			with open(out_file, 'w') as fd:
				# write header
				header = sorted(col_p.keys())
				fd.write(fdelim.join(header) + "\n")

				# write one row at a time
				# NOTE: we assume that col_p[p_id]["rows"] is sorted in increasing order
				row_iterators = {p:0 for p in col_p.keys()}
				row_count = 0
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

			plot_file="{}/l_{}_col_{}.{}".format(pattern_distribution_output_dir, level, col_id, plot_file_format)
			plot_pattern_distribution.main(in_file=out_file, out_file=plot_file, out_file_format=plot_file_format)

	@staticmethod
	def output_ngram_freq_masks(level, ngram_freq_masks, ngram_freq_masks_output_dir, plot_file_format="svg"):
		for col_id, values in ngram_freq_masks.items():
			out_file = "{}/l_{}_col_{}.csv".format(ngram_freq_masks_output_dir, level, col_id)
			with open(out_file, 'w') as fd:
				for v in values:
					fd.write(v + "\n")

			plot_file="{}/l_{}_col_{}.{}".format(ngram_freq_masks_output_dir, level, col_id, plot_file_format)
			plot_ngram_freq_masks.main(in_file=out_file, out_file=plot_file, out_file_format=plot_file_format)

	@staticmethod
	def output_expression_tree(expression_tree, output_dir, plot=True):
		expr_tree_out_file = os.path.join(output_dir, "expr_tree.json")
		with open(expr_tree_out_file, 'w') as f:
			json.dump(expression_tree.to_dict(), f, indent=2)
		if plot:
			expr_tree_plot_file = os.path.join(output_dir, "expr_tree.svg")
			plot_expression_tree(expression_tree, expr_tree_plot_file)


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
	parser.add_argument('--expr-tree-output-dir', dest='expr_tree_output_dir', type=str,
		help="Output dir to write expression tree data to",
		required=True)
	parser.add_argument('--pattern-distribution-output-dir', dest='pattern_distribution_output_dir', type=str,
		help="Output dir to write pattern distribution to")
	parser.add_argument('--ngram-freq-masks-output-dir', dest='ngram_freq_masks_output_dir', type=str,
		help="Output dir to write ngram frequency masks to")
	parser.add_argument("-F", "--fdelim", dest="fdelim",
		help="Use <fdelim> as delimiter between fields", default="|")
	parser.add_argument("--null", dest="null", type=str,
		help="Interprets <NULL> as NULLs", default="null")

	return parser.parse_args()


def read_data(driver, data_manager, fdelim):
	while True:
		line = driver.nextTuple()
		if line is None:
			break
		tpl = line.split(fdelim)
		data_manager.write_tuple(tpl)


def data_loop(data_manager, pd_engine, fdelim):
	data_manager.read_seek_set()
	while True:
		tpl = data_manager.read_tuple()
		if tpl is None:
			break
		pd_engine.feed_tuple(tpl)


def apply_expressions(expr_manager, in_data_manager, out_data_manager):
	total_tuple_count = 0
	valid_tuple_count = 0

	in_data_manager.read_seek_set()
	while True:
		tpl = in_data_manager.read_tuple()
		if tpl is None:
			break
		total_tuple_count += 1

		res = expr_manager.apply_expressions(tpl)
		if res is None:
			continue
		valid_tuple_count += 1

		(tpl_new, p_mask) = res

		out_data_manager.write_tuple(tpl_new)

	out_columns = expr_manager.get_out_columns()

	return out_columns


def init_pattern_detectors(in_columns, pattern_log, expression_tree, null_value):
	pd_obj_id = 0
	null_pattern_detector = NullPatternDetector(
		pd_obj_id, in_columns, pattern_log, expression_tree, null_value
		)
	pd_obj_id += 1
	constant_pattern_detector = ConstantPatternDetector(
		pd_obj_id, in_columns, pattern_log, expression_tree, null_value
		)
	pd_obj_id += 1
	number_as_string = NumberAsString(
		pd_obj_id, in_columns, pattern_log, expression_tree, null_value
		)
	pd_obj_id += 1
	string_common_prefix = StringCommonPrefix(
		pd_obj_id, in_columns, pattern_log, expression_tree, null_value
		)
	pd_obj_id += 1
	char_set_split = CharSetSplit(
		pd_obj_id, in_columns, pattern_log, expression_tree, null_value,
		default_placeholder="?",
		char_sets=[
			{"name": "digits", "placeholder": "D", "char_set": set(map(str, range(0,10)))},
			# {"name": "letters", "placeholder": "L", "char_set": set(string.ascii_lowercase + string.ascii_uppercase)},
			# TODO: play around with the char sets here
		],
		drop_single_char_pattern=True
		)
	pd_obj_id += 1
	ngram_freq_split = NGramFreqSplit(
		pd_obj_id, in_columns, pattern_log, expression_tree, null_value,
		n=3
		)
	# NOTE: don't forget to increment pd_obj_id before adding a new pattern

	pattern_detectors = [
		# null_pattern_detector,
		# constant_pattern_detector,
		number_as_string,
		# string_common_prefix,
		char_set_split,
		# ngram_freq_split,
	]
	return pattern_detectors


def build_expression_tree(args, in_data_manager, columns):
	in_columns = deepcopy(columns)
	expression_tree = ExpressionTree(in_columns)
	pattern_log = PatternLog()

	for it in range(MAX_ITERATIONS):
		print("\n\n=== ITERATION: it={} ===\n\n".format(it))

		# pattern detectors & selector
		pattern_detectors = init_pattern_detectors(in_columns, pattern_log, expression_tree, args.null)
		# pattern_selector = DummyPatternSelector(MIN_COL_COVERAGE)
		pattern_selector = CoveragePatternSelector(MIN_COL_COVERAGE)

		# init engine
		pd_engine = PatternDetectionEngine(in_columns, pattern_detectors)
		# feed data to engine
		data_loop(in_data_manager, pd_engine, args.fdelim)
		# get results from engine
		(patterns, total_tuple_count, valid_tuple_count) = pd_engine.get_patterns()
		# update pattern log
		pattern_log.update_log(patterns, pattern_detectors)

		# debug
		OutputManager.output_stats(in_columns, patterns)
		# end-debug

		# select patterns for each column
		expr_nodes = pattern_selector.select_patterns(patterns, in_columns, valid_tuple_count)

		# debug
		for en in expr_nodes: print(en)
		# end-debug

		# stop if no more patterns can be applied
		if len(expr_nodes) == 0:
			print("stop iteration: no more patterns can be applied")
			break

		# add expression nodes as a new level in the expression tree
		expression_tree.add_level(expr_nodes)

		# apply expression nodes
		out_data_manager = DataManager()
		expr_manager = ExpressionManager(in_columns, expr_nodes, args.null)
		out_columns = apply_expressions(expr_manager, in_data_manager, out_data_manager)

		# debug
		# for oc in out_columns: print(oc)
		# end-debug

# '''
		# output pattern distributions (for this level)
		if args.pattern_distribution_output_dir is not None:
			OutputManager.output_pattern_distribution(it, in_columns, patterns, args.pattern_distribution_output_dir)
		# output ngram frequency masks (for this level)
		if args.ngram_freq_masks_output_dir is not None:
			ngram_freq_split_pds = [pd for pd in pattern_detectors if isinstance(pd, NGramFreqSplit)]
			if len(ngram_freq_split_pds) > 0:
				if len(ngram_freq_split_pds) != 1:
					print("debug: more that one NGramFreqSplit pattern detector found; using the first one")
				ngram_freq_split = ngram_freq_split_pds[0]
				ngram_freq_masks = ngram_freq_split.get_ngram_freq_masks(delim=",")
				OutputManager.output_ngram_freq_masks(it, ngram_freq_masks, args.ngram_freq_masks_output_dir)
			else:
				print("debug: no NGramFreqSplit pattern detector used")
# '''

		# prepare next iteration
		in_data_manager = out_data_manager
		in_columns = out_columns
	else:
		print("stop iteration: MAX_ITERATIONS={} reached".format(MAX_ITERATIONS))

	return expression_tree


def main():
	args = parse_args()
	print(args)

	# read header and datatypes
	with open(args.header_file, 'r') as fd:
		header = list(map(lambda x: x.strip(), fd.readline().split(args.fdelim)))
	with open(args.datatypes_file, 'r') as fd:
		datatypes = list(map(lambda x: DataType.from_sql_str(x.strip()), fd.readline().split(args.fdelim)))
	if len(header) != len(datatypes):
		return RET_ERR

	# init columns
	columns = []
	for idx, col_name in enumerate(header):
		col_id = str(idx)
		columns.append(Column(col_id, col_name, datatypes[idx]))

	# read data
	in_data_manager = DataManager()
	try:
		if args.file is None:
			fd = os.fdopen(os.dup(sys.stdin.fileno()))
		else:
			fd = open(args.file, 'r')
		f_driver = FileDriver(fd)
		read_data(f_driver, in_data_manager, args.fdelim)
	finally:
		fd.close()

	# build expression tree
	expression_tree = build_expression_tree(args, in_data_manager, columns)

	# debug
	print("\n[levels]")
	for level in expression_tree.get_node_levels():
		print("level={}".format(level))
		# for node_id in level:
		# 	node = expression_tree.get_node(node_id)
		# 	print("node_id={}, node={}".format(node_id, node))
	# print("[all_columns]")
	# for col_id in expression_tree.columns:
	# 	print(expression_tree.get_column(col_id))
	print("[in_columns]")
	print(expression_tree.get_in_columns())
	print("[out_columns]")
	print(expression_tree.get_out_columns())
	print("[unused_columns]")
	print(expression_tree.get_unused_columns())
	# end-debug

	# output expression tree
	OutputManager.output_expression_tree(expression_tree, args.expr_tree_output_dir, plot=True)


if __name__ == "__main__":
	main()


"""
#[remote]
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=/scratch/bogdan/master-project/public_bi_benchmark-master_project/benchmark
#[local]
wbs_dir=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1
repo_wbs_dir=/ufs/bogdan/work/master-project/public_bi_benchmark-master_project/benchmark

================================================================================
wb=CommonGovernment
table=CommonGovernment_1
max_sample_size=$((1024*1024*10))
================================================================================
wb=Eixo
table=Eixo_1
max_sample_size=$((1024*1024*10))
================================================================================
wb=Arade
table=Arade_1
max_sample_size=$((1024*1024*10))


================================================================================
dataset_nb_rows=$(cat $repo_wbs_dir/$wb/samples/$table.linecount)
pattern_distr_out_dir=$wbs_dir/$wb/$table.patterns
ngram_freq_masks_output_dir=$wbs_dir/$wb/$table.ngram_freq_masks
expr_tree_output_dir=$wbs_dir/$wb/$table.expr_tree

#[sample]
./sampling/main.py --dataset-nb-rows $dataset_nb_rows --max-sample-size $max_sample_size --sample-block-nb-rows 64 --output-file $wbs_dir/$wb/$table.sample.csv $wbs_dir/$wb/$table.csv

#[pattern-detection]
mkdir -p $pattern_distr_out_dir $ngram_freq_masks_output_dir $expr_tree_output_dir && \
./pattern_detection/main.py --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv \
--datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv \
--pattern-distribution-output-dir $pattern_distr_out_dir \
--ngram-freq-masks-output-dir $ngram_freq_masks_output_dir \
--expr-tree-output-dir $expr_tree_output_dir \
$wbs_dir/$wb/$table.sample.csv

#[plot-expr-tree]
expr_tree_file=$expr_tree_output_dir/expr_tree.json
expr_tree_plot_file=$expr_tree_output_dir/expr_tree_manual.svg
./pattern_detection/plot_expression_tree.py --out-file $expr_tree_plot_file $expr_tree_file

#[scp-pattern-detection-results]
scp -r bogdan@bricks14:/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test/$wb/$table.patterns pattern_detection/output/
scp -r bogdan@bricks14:/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test/$wb/$table.ngram_freq_masks pattern_detection/output/
"""
