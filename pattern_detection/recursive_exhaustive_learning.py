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


def init_pattern_detectors(pattern_detectors, in_columns, pattern_log, expression_tree, null_value):
	pd_instances = []

	for pd_obj_id, (pd_name, pd_params) in enumerate(pattern_detectors.items()):
		class_obj = get_pattern_detector(pd_name)
		pd = class_obj(pd_obj_id, in_columns, pattern_log, expression_tree, null_value,
					   **pd_params)
		pd_instances.append(pd)

	return pd_instances


class RecursiveExhaustiveLearning(object):

	def __init__(self, args, in_data_manager, columns):
		if not args.test_sample:
			raise Exception("test sample not provided")
		print("test_sample: {}".format(args.test_sample))
		self.args = args
		self.in_data_manager = in_data_manager
		self.columns = columns


	def build_compression_tree(self):
		in_columns = deepcopy(self.columns)
		in_data_manager = self.in_data_manager
		expression_tree = ExpressionTree(in_columns, tree_type="compression")
		pattern_log = PatternLog()

		# TODO

		return expression_tree
