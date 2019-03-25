import os
import sys
from util import *


class PatternDetector(object):
	def __init__(self, null_value):
		self.null_value = null_value
		self.row_count = 0
		self.name = self.__class__.__name__

	def feed_tuple(self, tpl):
		self.row_count += 1

	def is_null(self, attr):
		return attr == self.null_value


class StringPatternDetector(PatternDetector):
	def __init__(self, columns, null_value):
		PatternDetector.__init__(self, null_value)

		self.columns = {}
		for idx, col in enumerate(columns):
			if not col.datatype.startswith("varchar"):
				continue
			col.patterns[self.name] = {"rows": []}
			self.columns[idx] = col


class NumberAsString(StringPatternDetector):
	def __init__(self, columns, null_value):
		StringPatternDetector.__init__(self, columns, null_value)

	def feed_tuple(self, tpl):
		StringPatternDetector.feed_tuple(self, tpl)

		for idx in self.columns.keys():
			attr = tpl[idx]
			if not (self.is_number(attr) or self.is_null(attr)):
				continue
			self.columns[idx].patterns[self.name]["rows"].append(self.row_count)

	def is_number(self, attr):
		try:
			float(attr)
			return True
		except ValueError as e:
			return False


class StringCommonPrefix(StringPatternDetector):
	def __init__(self, columns, null_value):
		StringPatternDetector.__init__(self, columns, null_value)

	def feed_tuple(self, tpl):
		StringPatternDetector.feed_tuple(self, tpl)

		for idx in self.columns.keys():
			attr = tpl[idx]
			# TODO
