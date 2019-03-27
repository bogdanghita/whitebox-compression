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
				"rows": []
			}


class NumberAsString(StringPatternDetector):
	def __init__(self, columns, null_value):
		StringPatternDetector.__init__(self, columns, null_value)

	def feed_tuple(self, tpl):
		StringPatternDetector.feed_tuple(self, tpl)

		for idx in self.columns.keys():
			attr = tpl[idx]
			if not (self.is_number(attr) or self.is_null(attr)):
				continue
			self.columns[idx]["rows"].append(
				dict(row_id=self.row_count, details=dict()))

	def is_number(self, attr):
		try:
			float(attr)
			return True
		except ValueError as e:
			return False

	def evaluate(self):
		res = []
		for idx, col in self.columns.items():
			if len(col["rows"]) == 0:
				continue
			score = 0 if self.row_count == 0 else (len(col["rows"]) / self.row_count)
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

	def feed_tuple(self, tpl):
		StringPatternDetector.feed_tuple(self, tpl)

		for idx in self.columns.keys():
			attr = tpl[idx]
			self.prefix_tree.insert(attr)

	def evaluate(self):
		return []
		# TODO
