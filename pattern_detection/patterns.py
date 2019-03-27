import os
import sys
from copy import deepcopy
from lib.util import *
from lib.prefix_tree import PrefixTree


class PatternDetector(object):
	def __init__(self, columns, null_value):
		self.null_value = null_value
		self.row_count = 0
		self.name = self.__class__.__name__

	def feed_tuple(self, tpl):
		self.row_count += 1

	def evaluate(self):
		'''Evaluates the pattern based on the data fed so far

		Returns:
			columns: list of columns analyzed by the pattern detector; format of a column:
				dict(
					col_id: int # id of the column
					score: float # number between 0 and 1 indicating how well the column fits this pattern
					rows: [dict(row_id: int, details: dict() # pattern-specific info), ...], # rows where the pattern applies
					details: dict() # pattern-specific info
				)
		'''
		return []

	def is_null(self, attr):
		return attr == self.null_value


class StringPatternDetector(PatternDetector):
	def __init__(self, columns, null_value):
		PatternDetector.__init__(self, columns, null_value)
		self.columns = {}
		for idx, col in enumerate(columns):
			if not col.datatype.startswith("varchar"):
				continue
			self.columns[idx] = {
				"info": deepcopy(col),
				"rows": [],
				"nulls": []
			}

	def handle_attr(self, attr, idx):
		'''Handles an attribute

		Returns:
			handled: boolean value indicating whether the attr was handled by this function or not
		'''
		if self.is_null(attr):
			self.columns[idx]["nulls"].append(dict(row_id=self.row_count, details=dict()))
			return True
		return False

	def feed_tuple(self, tpl):
		PatternDetector.feed_tuple(self, tpl)
		for idx in self.columns.keys():
			attr = tpl[idx]
			self.handle_attr(attr, idx)


class NumberAsString(StringPatternDetector):
	def __init__(self, columns, null_value):
		StringPatternDetector.__init__(self, columns, null_value)

	def is_number(self, attr):
		try:
			float(attr)
			return True
		except ValueError as e:
			return False

	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		if not self.is_number(attr):
			return True
		self.columns[idx]["rows"].append(
			dict(row_id=self.row_count, details=dict()))
		return True

	def evaluate(self):
		res = []
		for idx, col in self.columns.items():
			if len(col["rows"]) == 0:
				continue
			# NOTE: treat nulls as valid attrs when computing the score (they will be handled separately)
			score = 0 if self.row_count == 0 else (len(col["rows"]) + len(col["nulls"])) / self.row_count
			col_item = {
				"col_id": col["info"].col_id,
				"score": score,
				"rows": col["rows"],
				"details": dict(),
			}
			res.append(col_item)
		return res


class StringCommonPrefix(StringPatternDetector):
	def __init__(self, columns, null_value):
		StringPatternDetector.__init__(self, columns, null_value)
		self.prefix_tree = PrefixTree()

	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		self.prefix_tree.insert(attr)
		return True

	def evaluate(self):
		return []
		# TODO


class CharSetSplit(StringPatternDetector):
	def __init__(self, columns, null_value, char_sets):
		StringPatternDetector.__init__(self, columns, null_value)
		self.char_sets = char_sets
		self.occurrence_dict = {}

	def get_pattern_string(self, attr):
		pass
		# TODO

	def handle_attr(self, attr, idx):
		handled = StringPatternDetector.handle_attr(self, attr, idx)
		if handled:
			return True
		pattern_string = self.get_pattern_string(attr)
		if pattern_string not in self.occurrence_dict:
			self.occurrence_dict[pattern_string] = []
		self.occurrence_dict[pattern_string].append(self.row_count)
		return True

	def evaluate(self):
		return []
		# TODO
