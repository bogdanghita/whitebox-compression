import os
import sys
from copy import deepcopy
import math
from statistics import mean, median
import numpy as np
from collections import Counter, defaultdict
from overrides import overrides

SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1, SCRIPT_DIR)

from pattern_detection.lib.util import *


class Estimator(object):
	def __init__(self, columns, null_value):
		self.null_value = null_value
		self.row_count, valid_count = 0, 0
		self.name = self.get_name()
		self.columns = {}
		self.init_columns(columns)

	@classmethod
	def get_name(cls):
		return cls.__name__

	def init_columns(self, columns):
		for idx, col in enumerate(columns):
			if self.select_column(col):
				self.columns[idx] = self.empty_col_item(col)

	def select_column(self, col):
		return True

	@classmethod
	def empty_col_item(cls, col):
		return {
			"info": deepcopy(col)
		}

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		return False

	def feed_tuple(self, tpl):
		self.row_count += 1
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)

	def evaluate(self):
		'''Estimates size based on the data fed so far

		Returns:
			columns: dict(col_id, size) columns analyzed by the estimator, where:
				col_id: str # id of the column
				size: total estimated size of the compressed column (column, metadata)
		'''
		return {col_item["info"].col_id: self.evaluate_col(col_item) for col_item in self.columns.values()}

	def evaluate_col(self, col_item):
		"""Estimates column size

		Returns:
			size: total estimated size of the compressed column (values + metadata)
		"""
		null_size = self.get_null_size(col_item)
		values_size = self.get_values_size(col_item)
		metadata_size = self.get_metadata_size(col_item)

		return null_size + values_size + metadata_size

	def get_null_size(self, col_item):
		"""
		One bit for each value, indicating whether it is null or not

		Returns:
			size: size in bytes of the null values
		"""
		return float(1) * self.row_count / 8

	def get_values_size(self, col_item):
		raise Exception("Not implemented")

	def get_metadata_size(self, col_item):
		raise Exception("Not implemented")


class NoCompressionEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def select_column(self, col):
		return True

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		self.valid_count += 1

		return True

	@overrides
	def get_values_size(self, col_item):
		res = DatatypeAnalyzer.get_datatype_size(col_item["info"].datatype) * self.valid_count
		return res

	@overrides
	def get_metadata_size(self, col_item):
		return 0


class DictEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def select_column(self, col):
		return col.datatype.name.lower() == "varchar"

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["counter"] = Counter()
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		self.valid_count += 1

		col["counter"][attr] += 1

		return True

	@overrides
	def get_values_size(self, col_item):
		counter = col_item["counter"]
		nb_values = sum(counter.values())
		return self.get_col_size(counter.keys(), nb_values)

	@overrides
	def get_metadata_size(self, col_item):
		return self.get_dict_size(col_item["counter"].keys())

	@classmethod
	def get_dict_size(cls, map_keys):
		return sum([DatatypeAnalyzer.get_value_size(k) for k in map_keys])

	@classmethod
	def get_col_size(cls, map_keys, nb_values):
		required_bits = nb_bits_int(len(map_keys)-1)
		return math.ceil(float(required_bits) * nb_values / 8)


class RleEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def select_column(self, col):
		return NumericDatatype.is_numeric_datatype(col.datatype)

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["current"] = None
		res["max_run_size"] = 1
		res["max_length_size"] = 1
		return res

	def process_run_end(col_item):
		hint = DatatypeAnalyzer.get_value_size_hint(col_item["info"].datatype)
		run_size = DatatypeAnalyzer.get_value_size(col_item["current"]["run"], hint=hint)
		length_size = DatatypeAnalyzer.get_value_size(col_item["current"]["length"])

		if run_size > col_item["max_run_size"]:
			col_item["max_run_size"] = run_size
		if length_size > col_item["max_length_size"]:
			col_item["max_length_size"] = length_size

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		# NOTE: skip nulls
		if attr == self.null_value:
			return True
		self.valid_count += 1

		if col["current"] == None:
			col["current"] = {
				"run": attr,
				"length": 0
			}

		if attr == col["current"]["run"]:
			col["current"]["length"] += 1
		else:
			self.process_run_end(col)
			col["current"] = {
				"run": attr,
				"length": 1
			}

		return True

	@overrides
	def evaluate_col(self, col_item):
		# process current run before evaluation
		self.process_run_end(col_item)
		# call super method
		Estimator.evaluate_col(self, col_item)

	@overrides
	def get_values_size(self, col_item):
		size_B = (col_item["max_run_size"] + col_item["max_length_size"]) * self.valid_count
		return 0

	@overrides
	def get_metadata_size(self, col_item):
		return 0


class ForEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def select_column(self, col):
		return NumericDatatype.is_numeric_datatype(col.datatype)

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["reference"] = float("inf")
		res["attrs"] = []
		res["max_diff_size"] = 1
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		self.valid_count += 1

		# store attr to compute difference in the evaluation step
		col["attrs"].append(attr)

		# update reference value
		if attr < col["reference"]:
			col["reference"] = attr

		return True

	@overrides
	def evaluate_col(self, col_item):
		reference = col_item["reference"]
		hint = DatatypeAnalyzer.get_value_size_hint(col_item["info"].datatype)

		# compute differences & max_diff_size
		for attr in col_item["attrs"]:
			diff = attr - reference
			diff_size = DatatypeAnalyzer.get_value_size(diff, hint=hint)
			if diff_size > col_item["max_diff_size"]:
				col_item["max_diff_size"] = diff_size

		# call super method
		Estimator.evaluate_col(self, col_item)

	@overrides
	def get_values_size(self, col_item):
		return col_item["max_diff_size"] * self.valid_count

	@overrides
	def get_metadata_size(self, col_item):
		reference_size = 8
		return reference_size
