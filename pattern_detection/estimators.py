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

	def init_columns(self, columns):
		for idx, col in enumerate(columns):
			if self.select_column(col):
				self.columns[idx] = self.empty_col_item(col)

	@classmethod
	def select_column(cls, col):
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
			columns: dict(col_id, metadata) columns analyzed by the estimator, where:
				col_id: str # id of the column
				metadata: object used by EstimatorTest
		'''
		return {col_item["info"].col_id: self.evaluate_col(col_item) for col_item in self.columns.values()}

	def evaluate_col(self, col_item):
		raise Exception("Not implemented")


class EstimatorTrain(Estimator):
	def __init__(self, columns, null_value):
		Estimator.__init__(self, columns, null_value)

	@classmethod
	def get_name(cls):
		return cls.__name__[:-len("Train")]

	def evaluate_col(self, col_item):
		return self.get_metadata(col_item)

	def get_metadata(self, col_item):
		raise Exception("Not implemented")


class EstimatorTest(Estimator):
	def __init__(self, columns, metadata, null_value):
		Estimator.__init__(self, columns, null_value)
		self.metadata = metadata

	@classmethod
	def get_name(cls):
		return cls.__name__[:-len("Test")]

	@classmethod
	def supports_exceptions(cls):
		return False

	def evaluate_col(self, col_item):
		"""Estimates column size

		Returns:
			size: (values_size, metadata_size, exceptions_size, null_size)
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


# =========================================================================== #
# NoCompression
# =========================================================================== #


class NoCompressionEstimatorTrain(EstimatorTrain):
	def __init__(self, columns, null_value):
		EstimatorTrain.__init__(self, columns, null_value)

	@classmethod
	def select_column(cls, col):
		return True

	@classmethod
	def empty_col_item(cls, col):
		res = EstimatorTrain.empty_col_item(col)
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
	def get_metadata(self, col_item):
		return {}


class NoCompressionEstimatorTest(EstimatorTest):
	def __init__(self, columns, metadata, null_value):
		EstimatorTest.__init__(self, columns, metadata, null_value)

	@classmethod
	def supports_exceptions(cls):
		return False

	@classmethod
	def select_column(cls, col):
		return NoCompressionEstimatorTrain.select_column(col)

	@classmethod
	def empty_col_item(cls, col):
		res = EstimatorTest.empty_col_item(col)
		res["varchar"] = False
		if col.datatype.name == "varchar":
			res["varchar"] = True
			res["values_size"] = 0
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		col["valid_count"] += 1

		if col["varchar"]:
			# + 1 for null terminator
			col["values_size"] += DatatypeAnalyzer.get_value_size(attr, bits=False) + 1

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

		if col_item["varchar"]:
			res_B = col_item["values_size"]
		else:
			res_B = datatype_size * col_item["valid_count"]
		return res_B

	@overrides
	def get_metadata_size(self, col_item):
		return 0

	@overrides
	def get_exceptions_size(self, col_item):
		return 0


# =========================================================================== #
# Dict
# =========================================================================== #


class DictEstimatorTrain(EstimatorTrain):
	def __init__(self, columns, null_value, max_dict_size):
		EstimatorTrain.__init__(self, columns, null_value)
		self.max_dict_size = max_dict_size

	@classmethod
	def select_column(cls, col):
		return col.datatype.name.lower() == "varchar"

	@classmethod
	def empty_col_item(cls, col):
		res = EstimatorTrain.empty_col_item(col)
		res["counter"] = Counter()
		res["exceptions_size"] = 0
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
	def get_metadata(self, col_item):
		counter_optimized = self.optimize_dictionary(col_item["counter"], self.max_dict_size)
		return {
			"counter": counter_optimized
		}

	@classmethod
	def optimize_dictionary(cls, counter, size_max):
		"""
		Keep only the first n most common keys that fit in size_max
		"""
		counter_res = Counter()

		# all (key, count) pairs, sorted in decreasing order of count
		hist = counter.most_common()

		size = 0
		for key, count in hist:
			# NOTE: +1 byte for each key: null terminator (since keys are strings)
			size_key = DatatypeAnalyzer.get_value_size(key, bits=False) + 1
			if size + size_key > size_max:
				break
			size += size_key
			counter_res[key] = count

		return counter_res


class DictEstimatorTest(EstimatorTest):
	def __init__(self, columns, metadata, null_value):
		EstimatorTest.__init__(self, columns, metadata, null_value)

	@classmethod
	def supports_exceptions(cls):
		return True

	@classmethod
	def select_column(cls, col):
		return DictEstimatorTrain.select_column(col)

	@classmethod
	def empty_col_item(cls, col):
		res = EstimatorTest.empty_col_item(col)
		res["exception_count"] = 0
		res["exception_size"] = 0
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		col["valid_count"] += 1

		counter = self.metadata[col["info"].col_id]["counter"]

		if attr not in counter:
			col["exception_count"] += 1
			# +1 for null terminator
			col["exception_size"] += DatatypeAnalyzer.get_value_size(attr, bits=False) + 1

		return True

	@overrides
	def get_values_size(self, col_item):
		counter = self.metadata[col_item["info"].col_id]["counter"]
		if len(counter.keys()) == 0:
			return 0
		nb_values = col_item["valid_count"] - col_item["exception_count"]
		return self.get_col_size(counter.keys(), nb_values)

	@overrides
	def get_metadata_size(self, col_item):
		counter = self.metadata[col_item["info"].col_id]["counter"]
		res = self.get_dict_size(counter.keys())
		return res

	@overrides
	def get_exceptions_size(self, col_item):
		return col_item["exception_size"]

	@classmethod
	def get_dict_size(cls, map_keys):
		# NOTE: +1 byte for each key: null terminator (since keys are strings)
		return sum([DatatypeAnalyzer.get_value_size(k, bits=False) + 1 for k in map_keys])

	@classmethod
	def get_col_size(cls, map_keys, nb_values):
		required_bits = nb_bits_int(len(map_keys)-1)
		return math.ceil(float(required_bits) * nb_values / 8)


# =========================================================================== #
# Rle
# =========================================================================== #


class RleEstimatorTrain(EstimatorTrain):
	def __init__(self, columns, null_value):
		EstimatorTrain.__init__(self, columns, null_value)

	@classmethod
	def select_column(cls, col):
		return NumericDatatype.is_numeric_datatype(col.datatype)

	@classmethod
	def empty_col_item(cls, col):
		res = EstimatorTrain.empty_col_item(col)
		res["value_size_hint"] = DatatypeAnalyzer.get_value_size_hint(col.datatype)
		res["current"] = None
		res["max_run_size"] = 1
		res["max_length_size"] = 1
		return res

	def process_run_end(self, col_item):
		if col_item["valid_count"] == 0:
			return

		run_size = DatatypeAnalyzer.get_value_size(
					DatatypeCast.cast(col_item["current"]["run"], col_item["info"].datatype),
					hint=col_item["value_size_hint"], bits=True)
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
		return EstimatorTrain.evaluate_col(self, col_item)

	@overrides
	def get_metadata(self, col_item):
		max_length = (2 ** col_item["max_length_size"]) - 1
		return {
			"max_run_size": col_item["max_run_size"],
			"max_length_size": col_item["max_length_size"],
			"max_length": max_length
		}


class RleEstimatorTest(EstimatorTest):
	def __init__(self, columns, metadata, null_value):
		EstimatorTest.__init__(self, columns, metadata, null_value)
		self.metadata = metadata

	@classmethod
	def supports_exceptions(cls):
		return True

	@classmethod
	def select_column(cls, col):
		return RleEstimatorTrain.select_column(col)

	@classmethod
	def empty_col_item(cls, col):
		res = EstimatorTest.empty_col_item(col)
		res["value_size_hint"] = DatatypeAnalyzer.get_value_size_hint(col.datatype)
		res["current"] = None
		res["run_count"] = 0
		res["exception_count"] = 0
		return res

	def process_run_end(self, col_item):
		if col_item["valid_count"] == 0:
			return

		col_item["run_count"] += 1

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		# skip nulls
		if attr == self.null_value:
			return True
		col["valid_count"] += 1

		col_metadata = self.metadata[col["info"].col_id]
		max_run_size = col_metadata["max_run_size"]

		# skip exceptions
		value_size = DatatypeAnalyzer.get_value_size(
					DatatypeCast.cast(attr, col["info"].datatype),
					hint=col["value_size_hint"], bits=True)
		if value_size > max_run_size:
			col["exception_count"] += 1
			return True

		if col["current"] == None:
			col["current"] = {
				"run": attr,
				"length": 0
			}

		# NOTE: start a new run if max_length is exceeded
		if (attr == col["current"]["run"] and
			col["current"]["length"] < col_metadata["max_length"]):
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
		return EstimatorTest.evaluate_col(self, col_item)

	@overrides
	def get_values_size(self, col_item):
		if col_item["valid_count"] == 0:
			return 0

		col_metadata = self.metadata[col_item["info"].col_id]
		max_run_size = col_metadata["max_run_size"]
		max_length_size = col_metadata["max_length_size"]

		res_bits = (max_run_size + max_length_size) * col_item["run_count"]
		return math.ceil(res_bits / 8)

	@overrides
	def get_metadata_size(self, col_item):
		col_metadata = self.metadata[col_item["info"].col_id]
		max_run_size = col_metadata["max_run_size"]
		max_length_size = col_metadata["max_length_size"]

		return math.ceil((max_run_size + max_length_size) / 8)

	@overrides
	def get_exceptions_size(self, col_item):
		datatype_size = DatatypeAnalyzer.get_datatype_size(col_item["info"].datatype)
		return datatype_size * col_item["exception_count"]


# =========================================================================== #
# For
# =========================================================================== #


class ForEstimatorTrain(EstimatorTrain):
	def __init__(self, columns, null_value):
		EstimatorTrain.__init__(self, columns, null_value)

	@classmethod
	def select_column(cls, col):
		return NumericDatatype.is_numeric_datatype(col.datatype)

	@classmethod
	def empty_col_item(cls, col):
		res = Estimator.empty_col_item(col)
		res["reference"] = float("inf")
		res["attrs"] = []
		res["max_diff_size"] = 1
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
			if diff_size > col_item["max_diff_size"]:
				col_item["max_diff_size"] = diff_size

		# call super method
		return EstimatorTrain.evaluate_col(self, col_item)

	@overrides
	def get_metadata(self, col_item):
		return {
			"reference": col_item["reference"],
			"max_diff_size": col_item["max_diff_size"]
		}


class ForEstimatorTest(EstimatorTest):
	def __init__(self, columns, metadata, null_value):
		EstimatorTest.__init__(self, columns, metadata, null_value)
		self.metadata = metadata

	@classmethod
	def supports_exceptions(cls):
		return True

	@classmethod
	def select_column(cls, col):
		return ForEstimatorTrain.select_column(col)

	@classmethod
	def empty_col_item(cls, col):
		res = Estimator.empty_col_item(col)
		res["value_size_hint"] = DatatypeAnalyzer.get_value_size_hint(col.datatype)
		res["exception_count"] = 0
		return res

	@overrides
	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if attr == self.null_value:
			return True
		col["valid_count"] += 1

		col_metadata = self.metadata[col["info"].col_id]

		val = DatatypeCast.cast(attr, col["info"].datatype)
		diff = val - col_metadata["reference"]
		diff_size = DatatypeAnalyzer.get_value_size(diff, hint=col["value_size_hint"], bits=True)

		if diff_size > col_metadata["max_diff_size"]:
			col["exception_count"] += 1

		return True

	@overrides
	def get_values_size(self, col_item):
		if col_item["valid_count"] == 0:
			return 0

		col_metadata = self.metadata[col_item["info"].col_id]

		nb_elems = col_item["valid_count"] - col_item["exception_count"]
		res_bits = col_metadata["max_diff_size"] * nb_elems
		return math.ceil(res_bits / 8)

	@overrides
	def get_metadata_size(self, col_item):
		reference_size = 8
		return reference_size

	@overrides
	def get_exceptions_size(self, col_item):
		datatype_size = DatatypeAnalyzer.get_datatype_size(col_item["info"].datatype)
		return datatype_size * col_item["exception_count"]
