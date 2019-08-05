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


# debug
from plot_expression_tree import plot_expression_tree
import shutil
DEBUG_COUNTER = 0
def rm_rf(target):
	if os.path.exists(target):
		shutil.rmtree(target)
def mkdir_p(target):
	if not os.path.exists(target):
		os.makedirs(target)
# end-debug


pattern_detectors = {
	"ConstantPatternDetector": {"min_constant_ratio": 0.9},
	"DictPattern": {"max_dict_size": 64 * 1024, "max_key_ratio": 0.1},
	"NumberAsString": {},
	"CharSetSplit": {
		"default_placeholder": "?",
			"char_sets": [
				{"name": "digits", "placeholder": "D", "char_set": {"0","1","2","3","4","5","6","7","8","9"}},
			],
			"drop_single_char_pattern": True
	}
}


def init_pattern_detectors(col_in, expression_tree, null_value):
	pd_instances = []
	in_columns = [col_in]
	# NOTE: pattern log is not necessary for this algorithm
	pattern_log = PatternLog()

	for pd_obj_id, (pd_name, pd_params) in enumerate(pattern_detectors.items()):
		class_obj = get_pattern_detector(pd_name)
		pd = class_obj(pd_obj_id, [col_in], pattern_log, expression_tree, null_value,
					   **pd_params)
		pd_instances.append(pd)

	return pd_instances

def init_estimators_train(col_in, null_value, no_compression=False):
	columns = [col_in]
	if no_compression:
		res = [
			NoCompressionEstimatorTrain(columns, null_value)
		]
	else:
		max_dict_size = pattern_detectors["DictPattern"]["max_dict_size"]
		res = [
			NoCompressionEstimatorTrain(columns, null_value),
			# DictEstimatorTrain(columns, null_value,
			# 			  	   max_dict_size),
			RleEstimatorTrain(columns, null_value),
			ForEstimatorTrain(columns, null_value)
		]
	return res

def init_estimators_test(col_in, metadata, null_value, no_compression=False):
	columns = [col_in]
	if no_compression:
		res = [
			NoCompressionEstimatorTest(columns, metadata["NoCompressionEstimator"], null_value)
		]
	else:
		res = [
			NoCompressionEstimatorTest(columns, metadata["NoCompressionEstimator"], null_value),
			# DictEstimatorTest(columns, metadata["DictEstimator"], null_value),
			RleEstimatorTest(columns, metadata["RleEstimator"], null_value),
			ForEstimatorTest(columns, metadata["ForEstimator"], null_value)
		]
	return res


class RecursiveExhaustiveLearning(object):

	def __init__(self, args, in_data_manager, columns, config):
		if not args.test_sample:
			raise Exception("test sample not provided")
		print("test_sample: {}".format(args.test_sample))
		self.args = args
		self.columns = columns
		self.config = config
		self.in_data_manager_list = [DataManager() for col in self.columns]
		self._populate_data_manager_list(in_data_manager)

	def _populate_data_manager_list(self, in_data_manager):
		in_data_manager.read_seek_set()
		while True:
			tpl = in_data_manager.read_tuple()
			if tpl is None:
				break
			for idx in range(0, len(self.columns)):
				self.in_data_manager_list[idx].write_tuple(tpl[idx])

	def build_compression_tree(self):
		# debug
		global DEBUG_COUNTER
		DEBUG_COUNTER += 1
		out_dir = "/tmp/debug/build_compression_tree"
		rm_rf(out_dir)
		mkdir_p(out_dir)
		details_obj = {}
		# end-debug

		expression_tree_list = []
		for idx, col in enumerate(self.columns):
			# debug
			# if col.col_id not in ["19"]:
			# 	continue
			# end-debug

			tree_in = ExpressionTree([col], tree_type="compression")
			res = self._build_tree(col, tree_in, self.in_data_manager_list[idx])
			(size, tree_out, details) = res
			expression_tree_list.append(tree_out)
			# debug
			details_obj[col.col_id] = dict(size=sizeof_fmt(size), details=details)
			# end-debug

		# debug
		out_file = out_dir+"/details.json"
		with open(out_file, 'w') as f:
			json.dump(details_obj, f, indent=2)
		# end-debug

		expression_tree = ExpressionTree(self.columns, tree_type="compression")
		if len(expression_tree_list) == 0:
			return expression_tree
		else:
			# debug
			for idx, tree in enumerate(expression_tree_list):
				out_file = out_dir+"/{}.svg".format(idx)
				plot_expression_tree(tree, out_file)
			# end-debug

			for idx, tree in enumerate(expression_tree_list):
				expression_tree = ExpressionTree.merge(expression_tree, tree)

			# debug
			out_file = out_dir+"/merged.svg"
			plot_expression_tree(expression_tree, out_file)
			# end-debug

		return expression_tree

	def _compute_size(self, size_list, sample_tuple_count_test, full_file_linecount):
		(values_size, metadata_size, exceptions_size, null_size) = size_list
		sample_ratio = float(full_file_linecount) / sample_tuple_count_test

		# extrapolate the size for the full data
		full_size = sample_ratio * (values_size + exceptions_size + null_size)
		size_B = metadata_size + full_size

		return size_B

	def _estimator_evaluate(self, col_in, data_mgr_in):
		sol_list = []

		# estimator train
		estimator_train_list = init_estimators_train(col_in, self.args.null)
		# data loop
		data_mgr_in.read_seek_set()
		while True:
			attr = data_mgr_in.read_tuple()
			if attr is None:
				break
			for estimator in estimator_train_list:
				estimator.feed_tuple([attr])
		# retrieve metadata
		metadata = {}
		for estimator in estimator_train_list:
			res = estimator.evaluate()
			metadata[estimator.name] = res

		# estimator test
		estimator_test_list = init_estimators_test(col_in, metadata, self.args.null)
		# data loop
		data_mgr_in.read_seek_set()
		sample_tuple_count_test = 0
		while True:
			attr = data_mgr_in.read_tuple()
			if attr is None:
				break
			sample_tuple_count_test += 1
			for estimator in estimator_test_list:
				estimator.feed_tuple([attr])
		# evaluate estimators
		for estimator in estimator_test_list:
			res = estimator.evaluate()
			if col_in.col_id not in res:
				continue
			size_list = res[col_in.col_id]
			# size = sum(size_list)
			size = self._compute_size(size_list, sample_tuple_count_test, self.args.full_file_linecount)
			sol_list.append((size, estimator.name))

		return sol_list

	def _accept_result(self, result):
		coverage = result["coverage"]
		if coverage < self.config["min_col_coverage"]:
			return False
		return True

	def _pd_evaluate(self, col_in, pd, data_mgr_in):
		expr_node_list = []

		# feed attrs to the pattern detector
		data_mgr_in.read_seek_set()
		while True:
			attr = data_mgr_in.read_tuple()
			if attr is None:
				break
			pd.feed_tuple([attr])

		# evaluate pattern detector
		columns = pd.evaluate()
		if col_in.col_id not in columns:
			return []
		patterns = columns[col_in.col_id]

		# filter results (to limit the number of solutions)
		for p in patterns:
			if not self._accept_result(p):
				continue
			expr_node = pd.get_compression_node(p)
			expr_node_list.append(expr_node)

		return expr_node_list

	def _build_tree(self, col_in, tree_in, data_mgr_in):
		sol_list = []

		print("[_build_tree] col_in={}".format(col_in))

		# estimators
		sol_estimator_list = self._estimator_evaluate(col_in, data_mgr_in)
		for (size, estimator_name) in sol_estimator_list:
			sol_list.append((size, tree_in, {"estimator": estimator_name}))

		# limit tree depth
		if len(tree_in.get_node_levels()) < self.config["max_depth"]:
			# pattern detectors
			pd_list = init_pattern_detectors(col_in, tree_in, self.args.null)
			expr_node_list = []
			for pd in pd_list:
				expr_node_list_tmp = self._pd_evaluate(col_in, pd, data_mgr_in)
				expr_node_list.extend(expr_node_list_tmp)
			# apply pattern detectors & recurse
			for expr_node in expr_node_list:
				(size, tree_out, details) = self._apply_expr_node(col_in, expr_node, 
																  tree_in, 
																  data_mgr_in)
				sol_list.append((size, tree_out, {}))
		else:
			print("debug: max_depth={} reached".format(self.config["max_depth"]))

		return min(sol_list, key=lambda x: x[0])

	def _apply_expr_node(self, col_in, expr_node, tree_in, data_mgr_in):
		# update tree
		tree_out = deepcopy(tree_in)
		tree_out.add_level([expr_node])

		# apply expression node
		expr_manager = ExpressionManager([col_in], [expr_node], self.args.null)
		col_out_list = expr_manager.get_out_columns()
		data_mgr_out_list = [DataManager() for col_out in col_out_list]
		# data loop
		data_mgr_in.read_seek_set()
		while True:
			attr = data_mgr_in.read_tuple()
			if attr is None:
				break
			tpl_new = expr_manager.apply_expressions([attr])
			if tpl_new is None:
				continue
			for idx, col_out in enumerate(col_out_list):
				data_mgr_out_list[idx].write_tuple(tpl_new[idx])

		# recursive call for output columns
		sol_list = []
		for idx, col_out in enumerate(col_out_list):
			res = self._build_tree(col_out, tree_out, data_mgr_out_list[idx])
			sol_list.append(res)

		# debug
		global DEBUG_COUNTER
		DEBUG_COUNTER += 1
		out_dir = "/tmp/debug/apply_expr_node/{}".format(DEBUG_COUNTER)
		rm_rf(out_dir)
		mkdir_p(out_dir)
		# end-debug

		# merge resulting trees
		size = 0
		for idx, (size_c, tree_out_c, details) in enumerate(sol_list):
			# debug
			out_file = out_dir+"/{}.svg".format(idx)
			plot_expression_tree(tree_out_c, out_file)
			# end-debug

			size += size_c
			tree_out = ExpressionTree.merge(tree_out, tree_out_c)

		# debug
		out_file = out_dir+"/merged.svg"
		plot_expression_tree(tree_out, out_file)
		# end-debug

		return (size, tree_out, {})
