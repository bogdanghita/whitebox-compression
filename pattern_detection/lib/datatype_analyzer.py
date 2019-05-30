import os
import sys
import math
from decimal import *
from lib.util import *


MAX_DECIMAL_PRECISION = 18
RANGE_TINYINT = (-128, 127)
RANGE_SMALLINT = (-32768, 32767)


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


class DatatypeCast(object):
	pass


class NumericDatatypeCast(DatatypeCast):

	@staticmethod
	def to_decimal(val, precision, scale):
		precision, scale = int(precision), int(scale)
		dec = Decimal(val)
		dec_tpl = dec.as_tuple()
		exp = abs(dec_tpl.exponent)
		digits = len(dec_tpl.digits)
		# print("debug: ===")
		# print("val={}, p={}, s={}".format(val, precision, scale))
		# print("val={}, d={}, e={}".format(val, digits, exp))
		# print(val, dec_tpl)
		# print("end-debug: ===")
		if exp >= digits:
			digits = exp + 1
		if digits > precision or exp > scale:
			raise Exception("[to_decimal] Value does not match precision and/or scale")
		return dec

	@staticmethod
	def to_double(val):
		return float(val)


class DatatypeAnalyzer(object):
	def __init__(self):
		pass

	def feed_attr(self, attr):
		raise Exception("Not implemented")

	def get_datatype(self):
		raise Exception("Not implemented")

	@classmethod
	def get_size_disk(cls, val):
		"""
		Returns: size of val on disk, in bytes
		"""
		if isinstance(val, str):
			return len(val)
		if isinstance(val, int):
			# NOTE: 1 bit for sign
			bits = nb_bits(abs(val)) + 1
			return math.ceil(float(bits) / 8)
		if isinstance(val, float):
			# TODO: replace this with actual float size on disk
			return len(str(val))

		raise Exception("Unsupported datatype: {}".format(type(val)))


class NumericDatatypeAnalyzer(DatatypeAnalyzer):
	"""
	NOTE: current datatypes options: "decimal(x,y)", "double"
	TODO: add support for other numeric datatypes (e.g. tinyint, smallint, int, bigint, float)
	"""

	datatypes = {
		"decimal": {
			"cast": NumericDatatypeCast.to_decimal
		},
		"double": {
			"cast": NumericDatatypeCast.to_double
		}
	}
	illegal_chars = ['e', 'E', '_']
	unsupported_decimals = [Decimal("Infinity"), Decimal("-Infinity"), Decimal("NaN")]

	def __init__(self):
		DatatypeAnalyzer.__init__(self)
		self.decdigits_before_minmax = MinMax()
		self.decdigits_after_minmax = MinMax()
		self.num_scientific_notation = 0

	def feed_attr(self, attr):
		if self.is_scientific_notation(attr):
			self.num_scientific_notation += 1
			return
		try:
			dec = Decimal(attr)
			if dec in self.unsupported_decimals:
				raise Exception("Unsupported decimal: {}".format(dec))
			dec_tpl = dec.as_tuple()
			exp = abs(dec_tpl.exponent)
			digits = len(dec_tpl.digits)
			if exp >= digits:
				digits = exp + 1
			self.decdigits_before_minmax.push(digits - exp)
			self.decdigits_after_minmax.push(exp)
		except Exception as e:
			# print("error: unable to process attr: {}".format(attr))
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
			return DataType(name="decimal", params=[str(precision), str(scale)])

	@classmethod
	def cast(cls, val, datatype):
		'''
		Raises: Exception() if the value does not match the datatype
		'''
		if datatype.name not in cls.datatypes:
			raise Exception("[cast] Unsupported datatype: datatype={}".format(datatype))
		n_val = cls.datatypes[datatype.name]["cast"](val, *datatype.params)
		return n_val

	@classmethod
	def cast_preview(cls, val):
		''' Check if val is numeric
		NOTE: values that look like numbers in scientific notation are not considered valid (for now)
		Returns: numeric value if supported, None otherwise
		'''
		for c in val:
			if c in cls.illegal_chars:
				return None

		""" check if int
		NOTE: if float, the int() cast will fail
		"""
		try:
			n_val = int(val)
		except:
			pass
		# check if float
		try:
			n_val = float(val)
		except ValueError as e:
			return None

		# check format preserving
		str_n_val = cls.str(n_val)
		if val.find(str_n_val) < 0:
			return None

		# numeric value, prefix, suffix
		return n_val, val[:pos], val[pos+len(str_n_val):]

	@classmethod
	def str(cls, val):
		""" Casts numberic value to string
		NOTE: in the future we may want to have a custom str() implementation
		"""
		return str(val)

	@classmethod
	def is_scientific_notation(cls, attr):
		# check if a number represented as string is in scientific notation
		# TODO: implement a more reliable way of doing this
		if attr.lower().count('e') == 1:
			return True
		return False
