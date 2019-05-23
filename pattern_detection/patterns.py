import os
import sys
from copy import deepcopy
from statistics import mean, median
import numpy as np
from collections import Counter, defaultdict
from overrides import overrides
from lib.util import *
from lib.prefix_tree import PrefixTree
from lib.datatype_analyzer import *
from lib.nominal import *


class OperatorException(Exception):
	pass


class PatternDetector(object):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value):
		self.pd_obj_id = pd_obj_id
		self.pattern_log = pattern_log
		self.expr_tree = expr_tree
		self.null_value = null_value
		self.row_count = 0
		self.name = self.get_p_name()
		self.columns = {}

	@classmethod
	def get_p_name(cls):
		return cls.__name__

	def init_columns(self, columns):
		for idx, col in enumerate(columns):
			if self.select_column(col):
				self.columns[idx] = self.empty_col_item(col)

	def _select_column_norepeat(self, col):
		# do not try this pattern again on the same column
		p_log = self.pattern_log.get_log(col.col_id)
		pattern_signature = self.get_signature()
		if pattern_signature in p_log:
			return False
		return True

	def _select_column_norepeat_parent(self, col):
		# do not try this pattern if col is an output column of this exact same pattern
		pattern_signature = self.get_signature()
		et_col = self.expr_tree.get_column(col.col_id)
		if et_col is None:
			raise Exception("Column not present in the expression tree: col={}".format(col))
		# NOTE: exception columns can have multiple expr_n parents
		parent_expr_n_id_list = et_col["output_of"]
		if len(parent_expr_n_id_list) == 0:
			return True
		for parent_expr_n_id in parent_expr_n_id_list:
			parent_expr_n = self.expr_tree.get_node(parent_expr_n_id)
			if pattern_signature == parent_expr_n.pattern_signature:
				return False
		return True

	def _select_column_output_of(self, col, accept_out=set(), accept_ex=set(),
											reject_out=set(), reject_ex=set()):
		et_col = self.expr_tree.get_column(col.col_id)
		if et_col is None:
			raise Exception("Column not present in the expression tree: col={}".format(col))

		# NOTE: exception columns can have multiple expr_n parents
		parent_expr_n_id_list = et_col["output_of"]

		if len(parent_expr_n_id_list) == 0 and len(accept_out) + len(accept_ex) > 0:
			return False

		if OutputColumnManager.is_exception_col(col):
			for parent_expr_n_id in parent_expr_n_id_list:
				parent_expr_n = self.expr_tree.get_node(parent_expr_n_id)
				if parent_expr_n.p_name in accept_ex:
					return True
				if parent_expr_n.p_name in reject_ex:
					return False
		elif len(parent_expr_n_id_list) > 0:
			parent_expr_n = self.expr_tree.get_node(parent_expr_n_id_list[0])
			if parent_expr_n.p_name in accept_out:
				return True
			if parent_expr_n.p_name in reject_out:
				return False

		if len(accept_out) + len(accept_ex) > 0:
			return False

		return True

	def select_column(self, col):
		return True

	@classmethod
	def empty_col_item(cls, col):
		return {
			"info": deepcopy(col),
			"patterns": {
				"default": {"rows": [], "details": {}},
				# NOTE: pattern detectors with only one pattern should use the "default" pattern;
				# multi-pattern detectors should add a new entry for each pattern;
				# "default" can be used if there is a main pattern or it can be left empty
			}
		}

	def get_signature(self):
		return self.name

	@classmethod
	def is_null(cls, attr, null_value):
		return attr == null_value

	def feed_tuple(self, tpl):
		self.row_count += 1

	def evaluate(self):
		'''Evaluates the pattern based on the data fed so far

		Returns:
			columns: dict(col_id, patterns) columns analyzed by the pattern detector, where:
				col_id: str # id of the column
				patterns: list(
					dict(
						p_id: # id of the pattern
						p_name: # name of the pattern class
						rows: [row_id: int, ...], # rows where the pattern applies; indexed from 0
						coverage: float, # number between 0 and 1 indicating the proportion of non-exception rows
						null_coverage: float, # number between 0 and 1 indicating the proportion of nulls
						in_columns: [icol_1, icol_2, ...], # list of input columns; type: util.Column
						in_columns_consumed: [icol_1, icol_2, ...], # list of input columns that are consumed by this pattern (see NOTE-1 below); type: type(util.Column.col_id)
						res_columns: [rcol_1, rcol_2, ...], # list of resulting columns; type: util.Column
						ex_columns: [ecol_1, ecol_2, ...], # list of exception columns; type: util.Column
						operator_info: dict(), # operator parameters (used when applying the transformation)
						details: dict(), # pattern-specific info
						pattern_signature: str # unique signature of the pattern (for comparison purposes)
					)
				)

		NOTE-1: input columns can either be consumed or not; a consumed input column will be transformed and no longer be an output column of the expression tree; a column that is NOT consumed wis used just as a form of metadata for the transformation of other consumed columns AND will still remain and output column of the expression tree
		'''
		return dict()

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		'''
		Returns: a function with the following signature:
				 params: attrs
				 returns: attrs_out
				 raises: Exception if attrs are invalid (i.e. pattern and/or params are not applicable)
				 properties:
				 	len(attrs) = len(cols_in)
					len(attrs_out) = len(cols_out)
				 note-1: function assumes that the above properties are satisfied & does not check them
				 note-2: get_operator() function is meant to be called only in the initialization phase, not for every tuple; e.g. call get_operator once for each expression node and save the returned operator, then use it as many times as you want
		'''
		raise Exception("Not implemented")


class NullPatternDetector(PatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value):
		PatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)
		self.init_columns(columns)

	@overrides
	def select_column(self, col):
		return col.datatype.nullable

	@classmethod
	def empty_col_item(cls, col):
		return PatternDetector.empty_col_item(col)

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		if self.is_null(attr, self.null_value):
			self.columns[idx]["patterns"]["default"]["rows"].append(self.row_count-1)
		return True

	@overrides
	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)

	@overrides
	def evaluate(self):
		res = dict()
		for idx, col in self.columns.items():
			if len(col["patterns"]["default"]["rows"]) == 0:
				continue
			coverage = 0 if self.row_count == 0 else len(col["patterns"]["default"]["rows"]) / self.row_count
			p_item = {
				"p_id": "{}:default".format(self.name),
				"p_name": self.name,
				"coverage": coverage,
				"null_coverage": coverage,
				"rows": col["patterns"]["default"]["rows"],
				"in_columns": [col["info"]],
				"in_columns_consumed": [], # TODO
				"res_columns": [], # TODO
				"ex_columns": [], # TODO
				"operator_info": dict(), # TODO
				"details": dict(),
				"pattern_signature": self.get_signature()
			}
			patterns = [p_item]
			res[col["info"].col_id] = patterns
		return res


class ConstantPatternDetector(PatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value, min_constant_ratio):
		PatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)
		self.min_constant_ratio = min_constant_ratio
		self.init_columns(columns)

	@overrides
	def select_column(self, col):
		# do not try this pattern again on the same column
		if not self._select_column_norepeat(col):
			return False
		# do not try this pattern if col is an output column of this exact same pattern
		if not self._select_column_norepeat_parent(col):
			return False

		return True

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["nulls"] = []
		res["counter"] = Counter()
		res["attrs"] = defaultdict(list)
		return res

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		col = self.columns[idx]

		if self.is_null(attr, self.null_value):
			col["nulls"].append(self.row_count-1)
			return True

		col["counter"][attr] += 1
		col["attrs"][attr].append(self.row_count-1)

		return True

	@overrides
	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)

	def constant_compressible(self, col, constant, count):
		null_cnt = len(col["nulls"])
		constant_cnt = count
		notnull_cnt = self.row_count - null_cnt
		if notnull_cnt == 0:
			return False
		constant_ratio = float(constant_cnt) / notnull_cnt
		return constant_ratio >= self.min_constant_ratio

	def compute_coverage(self, col, constant, count):
		null_cnt = len(col["nulls"])
		valid_cnt = count
		total_cnt = self.row_count
		if total_cnt == 0:
			return 0
		return float(valid_cnt) / total_cnt

	def build_pattern_data(self, col, constant, count):
		ex_columns = []

		col["patterns"]["default"]["rows"] = col["attrs"][constant]
		coverage = self.compute_coverage(col, constant, count)
		null_coverage = 0 if self.row_count == 0 else len(col["nulls"]) / self.row_count

		# operator info
		operator_info = dict(name="map", constant=constant)

		# exception column info
		ecol = OutputColumnManager.get_exception_col(col["info"])
		ex_columns.append(ecol)

		# pattern data
		p_item = {
			"p_id": "{}:default".format(self.name),
			"p_name": self.name,
			"coverage": coverage,
			"null_coverage": null_coverage,
			"rows": col["patterns"]["default"]["rows"],
			"in_columns": [col["info"]],
			"in_columns_consumed": [col["info"]],
			"res_columns": [],
			"ex_columns": ex_columns,
			"operator_info": operator_info,
			"details": dict(),
			"pattern_signature": self.get_signature()
		}
		return p_item

	@overrides
	def evaluate(self):
		res = dict()

		for idx, col in self.columns.items():
			if len(col["counter"].keys()) == 0:
				continue

			constant, count = col["counter"].most_common(1)[0]
			if not self.constant_compressible(col, constant, count):
				continue

			# consider column if it is constant_compressible; no score needed
			p_item = self.build_pattern_data(col, constant, count)
			res[col["info"].col_id] = [p_item]

		return res

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		def operator(attrs):
			val = attrs[0]

			if cls.is_null(val, null_value):
				raise OperatorException("[{}] null value is not supported".format(cls.__name__))

			constant = operator_info["constant"]
			if val != constant:
				raise OperatorException("[{}] value not equal to constant: value={}".format(cls.__name__, val))

			return []

		return operator


class DictPattern(PatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value, max_dict_size):
		PatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)
		self.max_dict_size = max_dict_size
		self.init_columns(columns)

	@overrides
	def select_column(self, col):
		# NOTE: for now we only consider varchar columns
		if col.datatype.name != "varchar":
			return False

		# do not try this pattern again on the same column
		if not self._select_column_norepeat(col):
			return False
		# do not try this pattern if col is an output column of this exact same pattern
		if not self._select_column_norepeat_parent(col):
			return False

		""" do not try this pattern if col is an output column of ConstantPatternDetector
		(but it can be an exception column of it)
		"""
		if not self._select_column_output_of(col, reject_out={ConstantPatternDetector.get_p_name()}):
			return False

		return True

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["nulls"] = []
		res["counter"] = Counter()
		return res

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		col = self.columns[idx]

		if self.is_null(attr, self.null_value):
			col["nulls"].append(self.row_count-1)
			return True

		col["counter"][attr] += 1

		self.columns[idx]["patterns"]["default"]["rows"].append(self.row_count-1)

		return True

	@overrides
	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)

	def dict_compressible(self, col_item):
		counter = col_item["counter"]
		nb_keys = len(counter.keys())
		nb_elems = sum(counter.values())

		# check number of keys
		if nb_keys >= RANGE_SMALLINT[1]:
			return False

		# NOTE: for now we only consider varchar columns; thus, keys are strings

		# check dict size
		size_keys = sum([len(key) for key in counter.keys()])
		if size_keys > self.max_dict_size:
			return False

		# check total size of input vs total size of output
		size_out_col = nb_bits(size_keys-1) * nb_elems
		size_in_col = sum([len(key) * count for key, count in counter.items()])
		'''
		NOTE: we do not take into account the size of the dict, because the relation:
			size_in_col < size_out_col + size_keys
			may be False on the sample, but True on the full column
		'''
		# if size_in_col < size_out_col + size_keys:
		if size_in_col < size_out_col:
			return False

		return True

	def get_output_col_datatype(self, col_item):
		counter = col_item["counter"]
		nb_keys = len(counter.keys())

		if nb_keys-1 < RANGE_TINYINT[1]:
			name = "tinyint"
		elif nb_keys-1 < RANGE_SMALLINT[1]:
			name = "smallint"
		else:
			raise Exception("Dict size out of range: nb_keys={}".format(nb_keys))

		datatype = DataType(name=name)
		return datatype

	def compute_coverage(self, col):
		null_cnt = len(col["nulls"])
		valid_cnt = len(col["patterns"]["default"]["rows"])
		total_cnt = self.row_count
		if total_cnt == 0:
			return 0
		return float(valid_cnt) / total_cnt

	def build_pattern_data(self, col):
		res_columns = []
		coverage = self.compute_coverage(col)
		null_coverage = 0 if self.row_count == 0 else len(col["nulls"]) / self.row_count

		# operator info
		map_obj = {attr:pos for pos, attr in enumerate(col["counter"].keys())}
		operator_info = dict(name="map", map=map_obj)

		# new column info
		ncol_col_id = OutputColumnManager.get_output_col_id(
			in_col_id=col["info"].col_id,
			pd_id=self.pd_obj_id,
			p_id=0,
			out_col_idx=0)
		ncol_name = OutputColumnManager.get_output_col_name(
			in_col_name=col["info"].name,
			pd_id=self.pd_obj_id,
			p_id=0,
			out_col_idx=0)

		ncol_datatype = self.get_output_col_datatype(col)
		ncol_datatype.nullable = True
		ncol = Column(ncol_col_id, ncol_name, ncol_datatype)
		res_columns.append(ncol)

		# pattern data
		p_item = {
			"p_id": "{}:default".format(self.name),
			"p_name": self.name,
			"coverage": coverage,
			"null_coverage": null_coverage,
			"rows": col["patterns"]["default"]["rows"],
			"in_columns": [col["info"]],
			"in_columns_consumed": [col["info"]],
			"res_columns": res_columns,
			"ex_columns": [],
			"operator_info": operator_info,
			"details": dict(),
			"pattern_signature": self.get_signature()
		}
		return p_item

	@overrides
	def evaluate(self):
		res = dict()

		for idx, col in self.columns.items():
			if len(col["patterns"]["default"]["rows"]) == 0:
				continue
			if len(col["counter"].keys()) == 0:
				continue
			if not self.dict_compressible(col):
				continue

			# consider column if it is dict_compressible; no score needed
			p_item = self.build_pattern_data(col)
			res[col["info"].col_id] = [p_item]

		return res

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		def operator(attrs):
			val = attrs[0]

			if cls.is_null(val, null_value):
				raise OperatorException("[{}] null value is not supported".format(cls.__name__))

			map_obj = operator_info["map"]

			try:
				n_val = map_obj[val]
			except Exception as e:
				raise OperatorException("[{}] value not in dictionary: value={}, err={}".format(cls.__name__, val, e))

			attrs_out = [n_val]
			return attrs_out

		return operator


class StringPatternDetector(PatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value):
		PatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)

	@overrides
	def select_column(self, col):
		return col.datatype.name == "varchar"

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["nulls"] = []
		return res

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		if self.is_null(attr, self.null_value):
			self.columns[idx]["nulls"].append(self.row_count-1)
			return True
		return False

	@overrides
	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)


class NumberAsString(StringPatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value):
		StringPatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)
		self.init_columns(columns)

	@overrides
	def select_column(self, col):
		if not StringPatternDetector.select_column(self, col):
			return False

		# do not try this pattern again on the same column
		if not self._select_column_norepeat(col):
			return False
		# do not try this pattern if col is an output column of this exact same pattern
		if not self._select_column_norepeat_parent(col):
			return False

		return True

	@classmethod
	def empty_col_item(cls, col):
		res = StringPatternDetector.empty_col_item(col)
		res["ndt_analyzer"] = NumericDatatypeAnalyzer()
		return res

	@overrides
	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		if not NumericDatatypeAnalyzer.is_supported_number(attr):
			return True
		try:
			self.columns[idx]["ndt_analyzer"].feed_attr(attr)
		except Exception as e:
			return True
		self.columns[idx]["patterns"]["default"]["rows"].append(self.row_count-1)
		return True

	def compute_coverage(self, col):
		null_cnt = len(col["nulls"])
		valid_cnt = len(col["patterns"]["default"]["rows"])
		total_cnt = self.row_count
		if total_cnt == 0:
			return 0
		return float(valid_cnt) / total_cnt

	def build_pattern_data(self, col):
		res_columns, ex_columns = [], []
		coverage = self.compute_coverage(col)
		null_coverage = 0 if self.row_count == 0 else len(col["nulls"]) / self.row_count

		# operator info
		operator_info = dict(name="identity")

		# new column info
		# ncol_col_id = str(col["info"].col_id) + "_0"
		ncol_col_id = OutputColumnManager.get_output_col_id(
			in_col_id=col["info"].col_id,
			pd_id=self.pd_obj_id,
			p_id=0,
			out_col_idx=0)
		# ncol_name = str(col["info"].name) + "_0"
		ncol_name = OutputColumnManager.get_output_col_name(
			in_col_name=col["info"].name,
			pd_id=self.pd_obj_id,
			p_id=0,
			out_col_idx=0)

		ncol_datatype = col["ndt_analyzer"].get_datatype()
		ncol_datatype.nullable = True
		ncol = Column(ncol_col_id, ncol_name, ncol_datatype)
		res_columns.append(ncol)

		# exception column info
		ecol = OutputColumnManager.get_exception_col(col["info"])
		ex_columns.append(ecol)

		# pattern data
		p_item = {
			"p_id": "{}:default".format(self.name),
			"p_name": self.name,
			"coverage": coverage,
			"null_coverage": null_coverage,
			"rows": col["patterns"]["default"]["rows"],
			"in_columns": [col["info"]],
			"in_columns_consumed": [col["info"]],
			"res_columns": res_columns,
			"ex_columns": ex_columns,
			"operator_info": operator_info,
			"details": dict(),
			"pattern_signature": self.get_signature()
		}
		return p_item

	@overrides
	def evaluate(self):
		res = dict()
		for idx, col in self.columns.items():
			if len(col["patterns"]["default"]["rows"]) == 0:
				continue
			p_item = self.build_pattern_data(col)
			patterns = [p_item]
			res[col["info"].col_id] = patterns
		return res

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		'''
		NOTE: for now we use the identity operator; in the future we may want
			  to actually perform a cast to a numeric type, based on the column
			  datatype
		'''
		def operator(attrs):
			val, c_out = attrs[0], cols_out[0]
			if cls.is_null(val, null_value):
				raise OperatorException("[{}] null value is not supported".format(cls.__name__))
			# NOTE: this filters strings that look like numbers in scientific notation
			if not NumericDatatypeAnalyzer.is_supported_number(val):
				raise OperatorException("[{}] value is not numeric".format(cls.__name__))
			# check if value matches the the datatype of the output column; raise exception if not
			try:
				n_val = NumericDatatypeAnalyzer.cast(val, c_out.datatype)
			except Exception as e:
				raise OperatorException("[{}] value does not match datatype: value={}, datatype={}, err={}".format(cls.__name__, val, c_out.datatype, e))
			# NOTE: [?] in the future maybe return n_val instead of val
			attrs_out = [val]
			return attrs_out

		return operator


class StringCommonPrefix(StringPatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value):
		StringPatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)
		self.prefix_tree = PrefixTree()
		self.init_columns(columns)

	@overrides
	def select_column(self, col):
		if not StringPatternDetector.select_column(self, col):
			return False

		# do not try this pattern again on the same column
		if not self._select_column_norepeat(col):
			return False
		# # do not try this pattern if col is an output column of this exact same pattern
		# if not self._select_column_norepeat_parent(col):
		# 	return False

		return True

	@overrides
	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		self.prefix_tree.insert(attr)
		return True

	@overrides
	def evaluate(self):
		return dict()
		# TODO


class CharSetSplit(StringPatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value, default_placeholder, char_sets, empty_string_pattern="<empty_string>", drop_single_char_pattern=True):
		StringPatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)
		self.default_placeholder = default_placeholder
		self.char_sets = {c["placeholder"]:c for c in char_sets}
		self.empty_string_pattern = empty_string_pattern
		self.drop_single_char_pattern = drop_single_char_pattern
		self.init_columns(columns)

	@overrides
	def select_column(self, col):
		if not StringPatternDetector.select_column(self, col):
			return False

		# do not try this pattern again on the same column
		if not self._select_column_norepeat(col):
			return False
		# do not try this pattern if col is an output column of this exact same pattern
		if not self._select_column_norepeat_parent(col):
			return False

		return True

	@overrides
	def get_signature(self):
		char_sets_signature = ""
		for char_set in self.char_sets.values():
			c_set = char_set["char_set"]
			char_sets_signature += "[" + ",".join(sorted(list(c_set))) + "]"
		params_signature = "char_sets=[{}]".format(char_sets_signature)
		res = "{}:{}".format(self.name, params_signature)
		# print(res)
		return res

	def get_pattern_string(self, attr):
		pattern_string = []

		for c in attr:
			# look for c in each char set
			for ph, c_set in self.char_sets.items():
				if c not in c_set["char_set"]:
					continue
				break
			# use the default placeholder if not match
			else:
				ph = self.default_placeholder
			# update pattern string (if necessary)
			if len(pattern_string) == 0 or pattern_string[-1] != ph:
				pattern_string.append(ph)

		if len(pattern_string) == 0:
			return self.empty_string_pattern
		else:
			return "".join(pattern_string)

	@overrides
	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		col = self.columns[idx]

		ps = self.get_pattern_string(attr)
		if ps not in col["patterns"]:
			col["patterns"][ps] = {"rows": [], "details": {}}
		col["patterns"][ps]["rows"].append(self.row_count-1)
		return True

	def compute_coverage(self, col, pattern_s, pattern_s_data):
		null_cnt = len(col["nulls"])
		valid_cnt = len(pattern_s_data["rows"])
		total_cnt = self.row_count
		if total_cnt == 0:
			return 0
		return float(valid_cnt) / total_cnt

	def build_pattern_data(self, col, p_idx, pattern_s, pattern_s_data):
		res_columns, ex_columns = [], []
		coverage = self.compute_coverage(col, pattern_s, pattern_s_data)
		null_coverage = 0 if self.row_count == 0 else len(col["nulls"]) / self.row_count

		# operator info
		operator_info = dict(name="split", char_sets={}, pattern_string=pattern_s)
		for ph, cs in self.char_sets.items():
			new_cs = deepcopy(cs)
			new_cs["char_set"] = list(new_cs["char_set"])
			operator_info["char_sets"][ph] = new_cs
		operator_info["char_sets"][self.default_placeholder] = {"name": "default", "placeholder": self.default_placeholder, "char_set": []}

		# new columns info
		for idx, ph in enumerate(pattern_s):
			# ncol_col_id = str(col["info"].col_id) + "_" + str(idx)
			ncol_col_id = OutputColumnManager.get_output_col_id(
				in_col_id=col["info"].col_id,
				pd_id=self.pd_obj_id,
				p_id=p_idx,
				out_col_idx=idx)
			# ncol_name = str(col["info"].name) + "_" + str(idx)
			ncol_name = OutputColumnManager.get_output_col_name(
				in_col_name=col["info"].name,
				pd_id=self.pd_obj_id,
				p_id=p_idx,
				out_col_idx=idx)
			# NOTE: here we keep the original column datatype (i.e. varchar(x))
			# TODO: think of the possibility of giving a better datatype (e.g. varchar of shorter length, numeric datatype, etc.)
			# UPDATE: no need for his; recursive approach handles the case
			ncol_datatype = deepcopy(col["info"].datatype)
			ncol_datatype.nullable = True
			ncol = Column(ncol_col_id, ncol_name, ncol_datatype)
			res_columns.append(ncol)

		# exception column info
		ecol = OutputColumnManager.get_exception_col(col["info"])
		ex_columns.append(ecol)

		# pattern data
		p_item = {
			"p_id": "{}:{}".format(self.name, pattern_s),
			"p_name": self.name,
			"coverage": coverage,
			"null_coverage": null_coverage,
			"rows": pattern_s_data["rows"],
			"in_columns": [col["info"]],
			"in_columns_consumed": [col["info"]],
			"res_columns": res_columns,
			"ex_columns": ex_columns,
			"operator_info": operator_info,
			"details": dict(),
			"pattern_signature": self.get_signature()
		}
		return p_item

	@overrides
	def evaluate(self):
		res = dict()

		for idx, col in self.columns.items():
			patterns = []
			for ps, ps_data in col["patterns"].items():
				if len(ps_data["rows"]) == 0:
					continue
				if self.drop_single_char_pattern and (ps == self.empty_string_pattern or len(ps) == 1):
					continue
				p_idx = len(patterns)
				p_item = self.build_pattern_data(col, p_idx, ps, ps_data)
				patterns.append(p_item)
			res[col["info"].col_id] = patterns

		return res

	@classmethod
	def split_attr(cls, attr, pattern_string, char_sets, default_placeholder, default_inv_charset):
		default_exception = OperatorException("[{}] attr does not match pattern_string: attr={}, pattern_string={}".format(cls.__name__, attr, pattern_string))
		# NOTE: for now, we don't support empty value
		# NOTE: we assume len(pattern_string) > 0
		''' NOTE: we assume that the (pattern_string, char_sets) pair is
		valid, thus we don't check if ph is in char_sets '''

		# print("[split_attr] {}, {}, {}".format(attr, pattern_string, char_sets))

		if len(attr) == 0:
			raise default_exception

		attrs_out = []

		attr_idx = 0
		for ph in pattern_string:
			cnt = 0
			if ph == default_placeholder:
				while attr_idx < len(attr) and attr[attr_idx] not in default_inv_charset:
					cnt += 1
					attr_idx += 1
			else:
				c_set = char_sets[ph]
				while attr_idx < len(attr) and attr[attr_idx] in c_set["char_set"]:
					cnt += 1
					attr_idx += 1
			if cnt == 0:
				raise default_exception
			attrs_out.append(attr[attr_idx-cnt:attr_idx])
		if attr_idx < len(attr):
			raise default_exception

		return attrs_out

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		''' NOTE: c_set["char_set"] got serialized as a list; search is slow in list;
		it is converted back to a set here'''
		default_placeholder = None
		char_sets = {}
		for ph, c_set in operator_info["char_sets"].items():
			char_sets[c_set["placeholder"]] = deepcopy(c_set)
			char_sets[c_set["placeholder"]]["char_set"] = set(char_sets[c_set["placeholder"]]["char_set"])
			if c_set["name"] == "default":
				default_placeholder = c_set["placeholder"]
		default_inv_charset = {c for c_set in char_sets.values() for c in c_set["char_set"]}

		def operator(attrs):
			val = attrs[0]
			if cls.is_null(val, null_value):
				raise OperatorException("[{}] null value is not supported".format(cls.__name__))
			attrs_out = cls.split_attr(val, operator_info["pattern_string"], char_sets, default_placeholder, default_inv_charset)
			return attrs_out

		return operator


class NGramFreqSplit(StringPatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value, n, case_sensitive=False):
		StringPatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)
		self.n = n
		self.case_sensitive = case_sensitive
		self.init_columns(columns)

	@overrides
	def select_column(self, col):
		if StringPatternDetector.select_column(self, col) == False:
			return False

		# do not try this pattern again on the same column
		if not self._select_column_norepeat(col):
			return False
		# # do not try this pattern if col is an output column of this exact same pattern
		# if not self._select_column_norepeat_parent(col):
		# 	return False

		return True

	@classmethod
	def empty_col_item(cls, col):
		res = StringPatternDetector.empty_col_item(col)
		res["ngrams"] = {
			"freqs": {},
			"avg_freq": 0,
			"median_freq": 0
		}
		res["attrs"] = []
		return res

	def get_ngrams(self, attr):
		if not self.case_sensitive:
			attr = attr.lower()

		for i in range(len(attr) - self.n + 1):
			ng = attr[i:i + self.n]
			yield ng

	def get_ngram_freq_mask(self, attr, ngrams, delim="-"):
		if not self.case_sensitive:
			attr = attr.lower()
		ngfm = []

		for i in range(len(attr) - self.n + 1):
			ng = attr[i:i + self.n]
			ng_freq = 0 if ng not in ngrams else ngrams[ng]
			ngfm.append(str(ng_freq))

		return delim.join(ngfm)

	@overrides
	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		col = self.columns[idx]

		# update ngram frequencies
		ngrams = self.get_ngrams(attr)
		for ng in ngrams:
			if ng not in col["ngrams"]["freqs"]:
				col["ngrams"]["freqs"][ng] = 0
			col["ngrams"]["freqs"][ng] += 1

		# store the attributes for the evaluate() step
		# TODO: store these values globally, i.e. only once, to avoid storing them twice if more pattern detectors need to do this
		col["attrs"].append(attr)

		return True

	@overrides
	def evaluate(self):
		res = dict()

		# for col in self.columns.values():
		# 	if len(col["ngrams"]["freqs"].keys()) == 0:
		# 		# TODO: do something else here? check with the evaluate() output format
		# 		continue
		# 	print(col["info"], col["attrs"][:10])
		# 	col["ngrams"]["avg_freq"] = mean(col["ngrams"]["freqs"].values())
		# 	col["ngrams"]["median_freq"] = median(col["ngrams"]["freqs"].values())
		# 	print(col["ngrams"]["avg_freq"], col["ngrams"]["median_freq"])
		# 	print(sorted(col["ngrams"]["freqs"].values()))
		# 	for attr in col["attrs"]:
		# 		ngfm = self.get_ngram_freq_mask(attr, col["ngrams"]["freqs"])
		# 		print(attr, ngfm, col["ngrams"]["avg_freq"], col["ngrams"]["median_freq"])

		return res

	def get_ngram_freq_masks(self, delim=","):
		res = {}
		for col in self.columns.values():
			if len(col["ngrams"]["freqs"].keys()) == 0:
				continue
			# NOTE-1: we want to return a generator, thus the usage of round brackets
			# See: https://code-maven.com/list-comprehension-vs-generator-expression
			# NOTE-2: we also want to create a closure for the col variable
			res[col["info"].col_id] = (lambda col: (self.get_ngram_freq_mask(attr, col["ngrams"]["freqs"], delim=delim) for attr in col["attrs"]))(col)
		return res


class ColumnCorrelation(PatternDetector):
	def __init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value, min_corr_coef):
		PatternDetector.__init__(self, pd_obj_id, columns, pattern_log, expr_tree, null_value)
		self.min_corr_coef = min_corr_coef
		self.init_columns(columns)

	@overrides
	def init_columns(self, columns):
		PatternDetector.init_columns(self, columns)
		for source_idx, source_col in self.columns.items():
			for target_idx, target_col in self.columns.items():
				target_col_id = target_col["info"].col_id
				source_col["corr_counters"][target_col_id] = defaultdict(Counter)
		# print([col["info"].col_id for col in self.columns.values()])

	@overrides
	def select_column(self, col):
		# do not try this pattern again on the same column
		if not self._select_column_norepeat(col):
			return False
		""" only select if col is an output column of DictPattern
		(and not an exception column of it)
		"""
		return self._select_column_output_of(col, accept_out={DictPattern.get_p_name()})

	@classmethod
	def empty_col_item(cls, col):
		res = PatternDetector.empty_col_item(col)
		res["nulls"] = []
		res["attrs"] = []
		# see init_columns for corr_counters structure
		res["corr_counters"] = {}
		return res

	def handle_attr(self, attr, idx):
		col = self.columns[idx]

		if self.is_null(attr, self.null_value):
			col["nulls"].append(self.row_count-1)

		# store the attributes for the evaluate() step
		# TODO: store these values globally, i.e. only once, to avoid storing them twice if more pattern detectors need to do this
		col["attrs"].append(attr)

		return True

	@overrides
	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)

		"""
		[null-handling] also see notes-week_16
		Let X and Y be 2 columns; let X determine Y; let X_val and Y_val be
			values in X and Y on the same row;
		- If X_val is null: Both in the detection and compression phase:
			treat it like a normal value; i.e. nulls on col X can determine
			values in col Y; they are taken into account when computing the
			correlation coefficient the null value can appear on the map
		- If Y_val is null:
			- Detection phase: do NOT count it as an exception
			- Compression phase: they will be treated as exceptions and will
				be stored in the exception column; but there is no penalty,
				because even if it were not an exception, the exception column
				will contain a null at that position
		"""

		for source_idx in self.columns.keys():
			source_col, source_attr = self.columns[source_idx], tpl[source_idx]
			self.handle_attr(source_attr, source_idx)

			# for every 2 columns: update correlation counters
			for target_idx in self.columns.keys():
				# NOTE: for now also do it for same column for validation purposes
				# if source_idx == target_idx:
				# 	continue
				target_col, target_attr = self.columns[target_idx], tpl[target_idx]
				target_col_id = target_col["info"].col_id
				# update counter
				source_col["corr_counters"][target_col_id][source_attr][target_attr] += 1

	def evaluate_correlation(self, source_col, target_col):
		source_col_id, target_col_id = source_col["info"].col_id, target_col["info"].col_id
		corr_counters = source_col["corr_counters"][target_col_id]

		corr_map, corr_count = {}, 0
		for source_attr, counter in corr_counters.items():
			null_count = counter[self.null_value]
			(target_attr, count) = counter.most_common(1)[0]
			if target_attr == self.null_value:
				count = 0
			# NOTE: see [null-handling] info in feed_tuple()
			corr_count += count + null_count
			corr_map[source_attr] = target_attr

		total_cnt = self.row_count
		corr_coef = corr_count / total_cnt if total_cnt > 0 else 0.0

		return (corr_coef, corr_map)

	def select_correlation(self, corr_coef, corr_map):
		return corr_coef >= self.min_corr_coef

	def fill_in_rows(self, target_col, source_col, corr_map):
		source_col_id = source_col["info"].col_id
		rows = []
		for idx, (s_attr, t_attr) in enumerate(zip(source_col["attrs"], target_col["attrs"])):
			if corr_map[s_attr] == t_attr:
				rows.append(idx)
		if source_col_id not in target_col["patterns"]:
			target_col["patterns"][source_col_id] = {"rows": [], "details": {}}
		target_col["patterns"][source_col_id]["rows"] = rows

	def compute_coverage(self, target_col, source_col):
		source_col_id = source_col["info"].col_id

		valid_cnt = len(target_col["patterns"][source_col_id]["rows"])
		total_cnt = self.row_count
		if total_cnt == 0:
			return 0
		return float(valid_cnt) / total_cnt

	def build_pattern_data(self, col, p_idx, source_col, corr_coef, corr_map):
		ex_columns = []
		coverage = self.compute_coverage(col, source_col)
		null_coverage = 0 if self.row_count == 0 else len(col["nulls"]) / self.row_count

		source_col_id = source_col["info"].col_id

		# operator info
		operator_info = dict(name="validate_map", corr_map=corr_map)

		# exception column info
		ecol = OutputColumnManager.get_exception_col(col["info"])
		ex_columns.append(ecol)

		# debug
		# print(col["info"].col_id, source_col["info"].col_id, corr_coef, coverage, null_coverage)
		# end-debug

		# pattern data
		p_item = {
			"p_id": "{}:{}".format(self.name, source_col_id),
			"p_name": self.name,
			"coverage": coverage,
			"null_coverage": null_coverage,
			"rows": col["patterns"][source_col_id]["rows"],
			"in_columns": [col["info"], source_col["info"]],
			"in_columns_consumed": [col["info"]],
			"res_columns": [],
			"ex_columns": ex_columns,
			"operator_info": operator_info,
			"details": dict(corr_coef=corr_coef,
							src_col_id=source_col_id),
			"pattern_signature": self.get_signature()
		}
		return p_item

	@overrides
	def evaluate(self):
		res = defaultdict(list)

		for source_idx, source_col in self.columns.items():
			for target_idx, target_col in self.columns.items():
				if source_idx == target_idx:
					continue
				(corr_coef, corr_map) = self.evaluate_correlation(source_col, target_col)

				if not self.select_correlation(corr_coef, corr_map):
					continue

				self.fill_in_rows(target_col, source_col, corr_map)

				p_idx = len(res[target_col["info"].col_id])
				p_item = self.build_pattern_data(target_col, p_idx, source_col, corr_coef, corr_map)

				res[target_col["info"].col_id].append(p_item)

		return res

	@classmethod
	def get_operator(cls, cols_in, cols_out, operator_info, null_value):
		"""
		[null-handling] see comment in feed_tuple()
		"""

		def operator(attrs):
			target_val, source_val = attrs[0], attrs[1]
			corr_map = operator_info["corr_map"]

			if not source_val not in corr_map:
				raise OperatorException("[{}] source_val not in correlation map: source_val={}".format(cls.__name__, source_val))
			if corr_map[source_val] != target_val:
				raise OperatorException("[{}] (source_val, target_val) does not match correlation map".format(cls.__name__))

			return []

		return operator

	def get_corr_coefs(self):
		corr_coefs = defaultdict(dict)
		selected_corrs = []

		for source_idx, source_col in self.columns.items():
			for target_idx, target_col in self.columns.items():
				source_col_id, target_col_id = source_col["info"].col_id, target_col["info"].col_id
				# NOTE: for now also do it for same column for validation purposes
				# if source_idx == target_idx:
				# 	continue
				(corr_coef, corr_map) = self.evaluate_correlation(source_col, target_col)
				corr_coefs[source_col_id][target_col_id] = corr_coef

				if source_idx != target_idx and self.select_correlation(corr_coef, corr_map):
					selected_corrs.append((source_col_id, target_col_id, corr_coef))
			# print(corr_coefs[source_col_id])

		return corr_coefs, selected_corrs


'''
================================================================================
'''
pattern_detectors = {
	"NullPatternDetector".lower(): NullPatternDetector,
	"ConstantPatternDetector".lower(): ConstantPatternDetector,
	"DictPattern".lower(): DictPattern,
	"NumberAsString".lower(): NumberAsString,
	"StringCommonPrefix".lower(): StringCommonPrefix,
	"CharSetSplit".lower(): CharSetSplit,
	"NGramFreqSplit".lower(): NGramFreqSplit,
	"ColumnCorrelation".lower(): ColumnCorrelation,
}
def get_pattern_detector(pattern_id):
	default_exception = Exception("Invalid pattern id")
	try:
		pn = pattern_id.split(":")[0]
	except Exception as e:
		raise default_exception

	pnl = pn.lower()
	if pnl in pattern_detectors:
		return pattern_detectors[pnl]

	raise default_exception
