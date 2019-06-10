import os
import sys
from copy import deepcopy
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
		self.row_count = 0
		self.columns = {}
		self.init_columns(columns)

	def init_columns(self, columns):
		for idx, col in enumerate(columns):
			if self.select_column(col):
				self.columns[idx] = self.empty_col_item(col)

	def select_column(self, col):
		return True

	@classmethod
	def empty_col_item(cls, col):
		return {
			"info": deepcopy(col),
			"null_count": 0
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
		Returns:
			size: size in bytes of the null values
		"""
		return float(1) * col_item["null_count"] / 8

	def get_values_size(self, col_item):
		raise Exception("Not implemented")

	def get_metadata_size(self, col_item):
		raise Exception("Not implemented")


class DictEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

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
			col["null_count"] += 1
			return True

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
		return sum([DatatypeAnalyzer.get_size_disk(k) for k in map_keys])

	@classmethod
	def get_col_size(cls, map_keys, nb_values):
		required_bits = nb_bits_int(len(map_keys)-1)
		return float(required_bits) * nb_values / 8


class RleEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	def select_column(self, col):
		return NumericDatatype.is_numeric_datatype(col.datatype)

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		# TODO
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			col["null_count"] += 1
			return True

		# TODO

		return True

	@overrides
	def get_values_size(self, col_item):
		# TODO
		return 0

	@overrides
	def get_metadata_size(self, col_item):
		# TODO
		return 0


class ForEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	def select_column(self, col):
		return NumericDatatype.is_numeric_datatype(col.datatype)

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		# TODO
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			col["null_count"] += 1
			return True

		# TODO

		return True

	@overrides
	def get_values_size(self, col_item):
		# TODO
		return 0

	@overrides
	def get_metadata_size(self, col_item):
		# TODO
		return 0