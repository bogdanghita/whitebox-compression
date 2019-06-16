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
from pattern_detection.lib.datatype_analyzer import *


class Estimator(object):
	def __init__(self, columns, null_value):
		self.null_value = null_value
		self.row_count = 0
		self.name = self.get_name()
		self.columns = {}
		self.init_columns(columns)

	@classmethod
	def supports_exceptions(cls):
		return False

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
			"info": deepcopy(col),
			"valid_count": 0
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
				size: (values_size, metadata_size, exceptions_size, null_size)
		'''
		return {col_item["info"].col_id: self.evaluate_col(col_item) for col_item in self.columns.values()}

	def evaluate_col(self, col_item):
		"""Estimates column size

		Returns:
			size: total estimated size of the compressed column in bytes (values + metadata)
		"""
		null_size = self.get_null_size(col_item)
		values_size = self.get_values_size(col_item)
		metadata_size = self.get_metadata_size(col_item)
		exceptions_size = self.get_exceptions_size(col_item)

		return (values_size, metadata_size, exceptions_size, null_size)

	def get_null_size(self, col_item):
		"""
		One bit for each value, indicating whether it is null or not

		Returns:
			nulls size in bytes
		"""
		size_bits = float(1) * self.row_count
		if self.supports_exceptions():
			size_bits *= 2
		return size_bits / 8

	def get_values_size(self, col_item):
		"""
		Returns:
			values size in bytes
		"""
		raise Exception("Not implemented")

	def get_metadata_size(self, col_item):
		"""
		Returns:
			metadata size in bytes
		"""
		raise Exception("Not implemented")

	def get_exceptions_size(self, col_item):
		"""
		Returns:
			exceptions size in bytes
		"""
		raise Exception("Not implemented")


class NoCompressionEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def supports_exceptions(cls):
		return False

	@overrides
	def select_column(self, col):
		return True

	@classmethod
	def empty_col_item(cls, col):
		res = Estimator.empty_col_item(col)
		res["value_size_hint"] = DatatypeAnalyzer.get_value_size_hint(col.datatype)
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		col["valid_count"] += 1

		return True

	@overrides
	def get_values_size(self, col_item):
		if col_item["valid_count"] == 0:
			return 0
		datatype_size = DatatypeAnalyzer.get_datatype_size(col_item["info"].datatype)
		
		# debug
		# print("[col_id={}] datatype_size={}, datatype={}".format(
		# 		col_item["info"].col_id,
		# 		datatype_size,
		# 		col_item["info"].datatype))
		# end-debug

		res_B = datatype_size * col_item["valid_count"]
		return res_B

	@overrides
	def get_metadata_size(self, col_item):
		return 0

	@overrides
	def get_exceptions_size(self, col_item):
		return 0


class BitsEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def supports_exceptions(cls):
		return True

	@overrides
	def select_column(self, col):
		return (NumericDatatype.is_numeric_datatype(col.datatype) or 
				col.datatype.name.lower() == "varchar")

	@classmethod
	def empty_col_item(cls, col):
		res = Estimator.empty_col_item(col)
		res["max_value_size"] = 1
		res["value_size_hint"] = DatatypeAnalyzer.get_value_size_hint(col.datatype)
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		col["valid_count"] += 1

		val = DatatypeCast.cast(attr, col["info"].datatype)
		val_size = DatatypeAnalyzer.get_value_size(val, hint=col["value_size_hint"], bits=True)
		if val_size > col["max_value_size"]:
			col["max_value_size"] = val_size

		return True

	@overrides
	def get_values_size(self, col_item):
		if col_item["valid_count"] == 0:
			return 0
		datatype_size = DatatypeAnalyzer.get_datatype_size(col_item["info"].datatype)
		
		# debug
		# print("[col_id={}] max_value_size={}, datatype_size={}, datatype={}".format(
		# 		col_item["info"].col_id,
		# 		col_item["max_value_size"],
		# 		datatype_size,
		# 		col_item["info"].datatype))
		# end-debug

		res_bits = col_item["max_value_size"] * col_item["valid_count"]
		return res_bits / 8

	@overrides
	def get_metadata_size(self, col_item):
		return 0

	@overrides
	def get_exceptions_size(self, col_item):
		"""
		NOTE: for now we do not have exceptions
		"""
		return 0


class DictEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def supports_exceptions(cls):
		return True

	@overrides
	def select_column(self, col):
		return col.datatype.name.lower() == "varchar"

	@classmethod
	def empty_col_item(cls, col):
		res = Estimator.empty_col_item(col)
		res["counter"] = Counter()
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		col["valid_count"] += 1

		col["counter"][attr] += 1

		return True

	@overrides
	def get_values_size(self, col_item):
		if col_item["valid_count"] == 0:
			return 0
		counter = col_item["counter"]
		nb_values = sum(counter.values())
		return self.get_col_size(counter.keys(), nb_values)

	@overrides
	def get_metadata_size(self, col_item):
		return self.get_dict_size(col_item["counter"].keys())

	@overrides
	def get_exceptions_size(self, col_item):
		"""
		NOTE: for now we do not have exceptions
		"""
		return 0

	@classmethod
	def get_dict_size(cls, map_keys):
		return sum([DatatypeAnalyzer.get_value_size(k, bits=False) for k in map_keys])

	@classmethod
	def get_col_size(cls, map_keys, nb_values):
		required_bits = nb_bits_int(len(map_keys)-1)
		return math.ceil(float(required_bits) * nb_values / 8)


class RleEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def supports_exceptions(cls):
		return True

	@overrides
	def select_column(self, col):
		return NumericDatatype.is_numeric_datatype(col.datatype)

	@classmethod
	def empty_col_item(cls, col):
		res = Estimator.empty_col_item(col)
		res["current"] = None
		res["max_run_size"] = 1
		res["max_length_size"] = 1
		res["run_count"] = 0
		return res

	def process_run_end(self, col_item):
		col_item["run_count"] += 1

		hint = DatatypeAnalyzer.get_value_size_hint(col_item["info"].datatype)
		run_size = DatatypeAnalyzer.get_value_size(
					DatatypeCast.cast(col_item["current"]["run"], col_item["info"].datatype), 
					hint=hint, bits=True)
		length_size = DatatypeAnalyzer.get_value_size(col_item["current"]["length"], 
													  signed=False, bits=True)

		# debug
		# print("[col_id={}][{}] col_item[\"current\"]={}, run_size={}, length_size={}".format(
		# 		col_item["info"].col_id, self.name, col_item["current"], run_size, length_size))
		# end-debug

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
		col["valid_count"] += 1

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
		return Estimator.evaluate_col(self, col_item)

	@overrides
	def get_values_size(self, col_item):
		if col_item["valid_count"] == 0:
			return 0
		res_bits = (col_item["max_run_size"] + col_item["max_length_size"]) * col_item["run_count"]
		return res_bits / 8

	@overrides
	def get_metadata_size(self, col_item):
		return 0

	@overrides
	def get_exceptions_size(self, col_item):
		"""
		NOTE: for now we do not have exceptions
		"""
		return 0


class ForEstimator(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@overrides
	def supports_exceptions(cls):
		return True

	@overrides
	def select_column(self, col):
		return NumericDatatype.is_numeric_datatype(col.datatype)

	@classmethod
	def empty_col_item(cls, col):
		res = Estimator.empty_col_item(col)
		res["reference"] = float("inf")
		res["attrs"] = []
		res["max_diff_size"] = 1
		# debug
		# res["max_diff"] = 1
		# res["max_attr"] = 0
		# end-debug
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		col["valid_count"] += 1

		# store attr to compute difference in the evaluation step
		col["attrs"].append(attr)

		val = DatatypeCast.cast(attr, col["info"].datatype)
		# update reference value
		if val < col["reference"]:
			col["reference"] = val

		return True

	@overrides
	def evaluate_col(self, col_item):
		reference = col_item["reference"]
		hint = DatatypeAnalyzer.get_value_size_hint(col_item["info"].datatype)

		# compute differences & max_diff_size
		for attr in col_item["attrs"]:
			val = DatatypeCast.cast(attr, col_item["info"].datatype)
			diff = val - reference
			diff_size = DatatypeAnalyzer.get_value_size(diff, hint=hint, bits=True)
			# debug
			# print("[col_id={}][{}] val={}, reference={}, diff={}, diff_size={}".format(
			# 		col_item["info"].col_id, self.name, val, reference, diff, diff_size))
			# end-debug
			if diff_size > col_item["max_diff_size"]:
				col_item["max_diff_size"] = diff_size
				# col_item["max_diff"] = diff
			# debug ^ & v
			# if abs(diff) > col_item["max_diff"]:
			# 	col_item["max_diff"] = diff
			# if val > col_item["max_attr"]:
			# 	col_item["max_attr"] = val
			# end-debug

		# call super method
		return Estimator.evaluate_col(self, col_item)

	@overrides
	def get_values_size(self, col_item):
		if col_item["valid_count"] == 0:
			return 0

		# debug
		# print("[col_id={}] reference={}, max_attr={}, max_diff_size={}, max_diff={}".format(
		# 		col_item["info"].col_id,
		# 		col_item["reference"],
		# 		col_item["max_attr"],
		# 		col_item["max_diff_size"], 
		# 		col_item["max_diff"]))
		# print("[col_id={}] value_size={}".format(
		# 		col_item["info"].col_id,
		# 		DatatypeAnalyzer.get_value_size(col_item["max_diff"])), bits=True)
		# end-debug

		res_bits = col_item["max_diff_size"] * col_item["valid_count"]
		return res_bits / 8

	@overrides
	def get_metadata_size(self, col_item):
		reference_size = 8
		return reference_size

	@overrides
	def get_exceptions_size(self, col_item):
		"""
		NOTE: for now we do not have exceptions
		"""
		return 0
