import os
import sys
from decimal import *
from lib.util import *


MAX_DECIMAL_PRECISION = 18


def is_scientific_notation(attr):
	# check if a number represented as string is in scientific notation
	# TODO: implement a more reliable way of doing this
	if attr.count('e') == 1:
		return True
	return False


class MinMax(object):
	def __init__(self):
		self.dmin = None
		self.dmax = None

	def push(self, x):
		if self.dmax is None or x > self.dmax:
			self.dmax = x
		if self.dmin is None or x < self.dmin:
			self.dmin = x

	def exists(self):
		return self.dmax is not None and self.dmin is not None

	def merge(self, other):
		self.push(other.dmin)
		self.push(other.dmax)


class DatatypeAnalyzer(object):
	def __init__(self):
		pass

	def feed_attr(self, attr):
		raise Exception("Not implemented")

	def get_datatype(self):
		raise Exception("Not implemented")


class NumericDatatypeAnalyzer(DatatypeAnalyzer):
	"""
	NOTE: current datatypes options: "decimal(x,y)", "double"
	TODO: add support for other numeric datatypes (e.g. tinyint, smallint, int, bigint, float)
	"""

	def __init__(self):
		DatatypeAnalyzer.__init__(self)
		self.decdigits_before_minmax = MinMax()
		self.decdigits_after_minmax = MinMax()
		self.num_scientific_notation = 0

	def feed_attr(self, attr):
		if is_scientific_notation(attr):
			self.num_scientific_notation += 1
			return
		try:
			dec = Decimal(attr)
			exp = abs(dec.as_tuple().exponent)
			digits = len(dec.as_tuple().digits)
			if exp >= digits:
				digits = exp + 1
			self.decdigits_before_minmax.push(digits - exp)
			self.decdigits_after_minmax.push(exp)
		except Exception as e:
			print("error: unable to process attr: {}".format(attr))
			raise e

	def get_datatype(self):
		if self.num_scientific_notation != 0:
			return "double"

		decdigits_before_dmax = self.decdigits_before_minmax.dmax
		if decdigits_before_dmax is None or decdigits_before_dmax < 0:
			decdigits_before_dmax = 1
		decdigits_after_dmax = self.decdigits_after_minmax.dmax
		if decdigits_after_dmax is None or decdigits_after_dmax < 0:
			decdigits_after_dmax = 0

		precision = decdigits_before_dmax + decdigits_after_dmax
		scale = decdigits_after_dmax

		if precision > MAX_DECIMAL_PRECISION:
			return DataType(name="double")
		else:
			return DataType(name="decimal", params=[precision, scale])
