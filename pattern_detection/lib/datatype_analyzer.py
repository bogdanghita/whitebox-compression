import os
import sys
import math
from decimal import *
from lib.util import *


MAX_DECIMAL_PRECISION = 18

"""
source:
https://communities.actian.com/s/article/SQL-Data-Types-and-Ingres-Vectorwise
https://docs.huihoo.com/ingres/ingres2006r2-guides/OpenAPI%20User%20Guide/1371.htm
https://www.geeksforgeeks.org/data-types-in-c/
"""
DATATYPES = {
	"boolean": dict(size=1),
	"integer": dict(size=4),
	"int": dict(size=4),
	"tinyint": dict(size=1, range=(-128, 127)),
	"smallint": dict(size=2, range=(-32768, 32767)),
	"bigint": dict(size=8),
	"decimal": dict(), # size computed in DatatypeAnalyzer.get_datatype_size()
	"float": dict(size=4),
	"float4": dict(size=4),
	"real": dict(size=4),
	"double": dict(size=8),
	"float8": dict(size=8),
	"varchar": dict(), # size computed in DatatypeAnalyzer.get_datatype_size()
	"date": dict(size=12), # size of ingresdate
	"time": dict(size=10),
	"timestamp": dict(size=14),
}


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


class NumericDatatype(object):
	numeric_datatypes = {
		"decimal",
		"tinyint",
		"smallint",
		"int",
		"bigint",
		"float",
		"real",
		"float8",
		"float4",
		"integer",
		"double"
	}

	@classmethod
	def is_numeric_datatype(cls, datatype):
		return datatype.name.lower() in cls.numeric_datatypes


class DatatypeCast(object):
	@classmethod
	def cast(cls, val, datatype):
		cast_f_list = [
			({"integer", "int", "tinyint", "smallint", "bigint"}, 
				int),
			({"float", "float4", "float8", "double", "real", "double"}, 
				float),
			({"decimal"}, 
				lambda x: cls.to_decimal(x, *datatype.params)),
			({"varchar", "date", "time", "timestamp"}, 
				str),
		]

		for dts, cast_f in cast_f_list:
			if datatype.name.lower() in dts:
				return cast_f(val)
		raise Exception("[cast] Unsupported datatype: datatype={}".format(datatype))

	@classmethod
	def to_decimal(cls, val, precision, scale):
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


class DatatypeAnalyzer(object):
	def __init__(self):
		pass

	def feed_attr(self, attr):
		raise Exception("Not implemented")

	def get_datatype(self):
		raise Exception("Not implemented")

	@classmethod
	def get_datatype_size(cls, datatype, bits=False):
		"""
		Returns: size of datatype on disk, in bytes if not bits else bits

		NOTE: for decimal see:
		https://docs.huihoo.com/ingres/ingres2006r2-guides/OpenAPI%20User%20Guide/1371.htm
		"""
		if datatype.name.lower() not in DATATYPES:
			raise Exception("Unsupported datatype: {}".format(datatype))
		
		if datatype.name.lower() == "decimal":
			precision = int(datatype.params[0])
			res_B = math.floor(precision / 2) + 1
		elif datatype.name.lower() == "varchar":
			res_B = int(datatype.params[0])
		else:
			res_B = DATATYPES[datatype.name.lower()]["size"]

		return res_B * 8 if bits else res_B

	@classmethod
	def get_value_size(cls, val, hint=None, signed=True, bits=False):
		"""
		Returns: size of val on disk, in bytes if not bits else bits
		"""
		if isinstance(val, str):
			res_B = max(1, len(val))
			return res_B * 8 if bits else res_B

		if isinstance(val, int):
			size_bits = nb_bits_int(abs(val))
			if signed: # 1 bit for sign
				size_bits += 1
			return size_bits if bits else math.ceil(float(size_bits) / 8)

		# if isinstance(val, Decimal):
		# 	dec = val.as_tuple()
		# 	digits, exponent = dec.digits, dec.exponent
		# 	physical_val = int("".join([str(d) for d in digits]))
		# 	return cls.get_value_size(physical_val, bits)

		if isinstance(val, Decimal):
			dec = val.as_tuple()
			digits, exponent = dec.digits, dec.exponent
			precision, scale = len(digits), abs(exponent)
			if scale >= precision:
				precision = scale + 1
			datatype = DataType(name="decimal", params=[precision, scale])
			return cls.get_datatype_size(datatype, bits) 

		if isinstance(val, float):
			if hint == "float":
				res_B = DATATYPES["float"]["size"]
			elif hint == "double":
				res_B = DATATYPES["double"]["size"]
			# default to size of double
			elif hint is None:
				res_B = DATATYPES["double"]["size"]
			else:
				raise Exception("Invalid hint for float")
			return res_B * 8 if bits else res_B

		raise Exception("Unsupported datatype: {}".format(type(val)))

	@classmethod
	def get_value_size_hint(cls, datatype):
		hint_list = [
			({"float", "float4", "real"}, "float"),
			({"double", "float8"}, "double")
		]
		for dts, hint in hint_list:
			if datatype.name.lower() in dts:
				return hint
		return None


class NumericDatatypeAnalyzer(DatatypeAnalyzer):
	"""
	NOTE: current datatypes options: "decimal(x,y)", "double"
	TODO: add support for other numeric datatypes (e.g. tinyint, smallint, int, bigint, float)
	"""

	datatypes = {"decimal", "double"}
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
		if datatype.name.lower() not in cls.datatypes:
			raise Exception("[cast] Unsupported datatype: datatype={}".format(datatype))
		# n_val = cls.datatypes[datatype.name.lower()]["cast"](val, *datatype.params)
		n_val = DatatypeCast.cast(val, datatype)
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
		n_val = None
		try:
			n_val = int(val)
		except:
			pass
		# check if float
		if n_val is None:
			try:
				n_val = float(val)
			except ValueError as e:
				return None

		# check format preserving
		str_n_val = cls.str(n_val)
		pos = val.find(str_n_val)
		if pos < 0:
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
