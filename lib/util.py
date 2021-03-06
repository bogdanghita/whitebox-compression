import sys
import os
import json
import re
import math
from copy import deepcopy


class DebugException(Exception):
	pass


def sizeof_fmt(num, suffix='B'):
	for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
		if abs(num) < 1024.0:
			return "%.1f%s%s" % (num, unit, suffix)
		num /= 1024.0
	return "%.1f%s%s" % (num, 'Yi', suffix)


def nb_bits_int(num):
	if num < 0:
		raise Exception("Unsupported: negative number")
	if num == 0:
		return 1
	return math.ceil(math.log(num+1, 2))


def eprint(s):
	sys.stderr.write(s + "\n")
	sys.stderr.flush()


def to_row_mask(selected_rows, nb_rows_total):
	row_mask = ['0'] * nb_rows_total
	try:
		for r in selected_rows:
			row_mask[r] = '1'
	except Exception as e:
		print("r={}, nb_rows_total={}".format(r, nb_rows_total))
		raise e
	return "".join(row_mask)


class FileDriver(object):
	def __init__(self, fd):
		self.fd = fd
		self.done = False

	def nextTuple(self):
		if self.done:
			return None

		l = self.fd.readline()
		if not l:
			self.done = True
			return None

		return l.rstrip('\r\n')


class DataManager(object):
	def __init__(self):
		self.tuples = []
		self.read_seek_set()

	def read_seek_set(self):
		self.idx = 0

	def read_tuple(self):
		if self.idx == len(self.tuples):
			return None
		tpl = self.tuples[self.idx]
		self.idx += 1
		return tpl

	def write_tuple(self, tpl):
		self.tuples.append(tpl)


class PatternLog(object):
	"""
	Keeps track of the result of applying patterns to every column
	"""
	def __init__(self):
		self.columns = {}

	def update_log(self, patterns, pattern_detectors):
		pattern_detectors = {pd.name: pd for pd in pattern_detectors}
		for pd in patterns.values():
			pd_name = pd["name"]
			if pd_name not in pattern_detectors:
				raise Exception("Unknown pattern detector: pd_name={}".format(pd_name))
			pd_obj = pattern_detectors[pd_name]
			for col_id, col_p_list in pd["columns"].items():
				if col_id not in self.columns:
					self.columns[col_id] = set()
				self.columns[col_id].add(pd_obj.get_signature())

	def get_log(self, col_id=None):
		if col_id is None:
			return self.columns
		if col_id not in self.columns:
			return {}
		return self.columns[col_id]


class DataType(object):
	regex_sql = re.compile(r'^(.*?)(\(.*?\))?( not null)?,?$')

	def __init__(self, name, nullable=True, params=[]):
		self.name = name
		self.nullable = nullable
		self.params = params

	def __repr__(self):
		return "DataType(name=%r,nullable=%r,params=%r)" % (self.name, self.nullable, self.params)

	@classmethod
	def from_sql_str(cls, sql_str):
		m = cls.regex_sql.match(sql_str.lower())
		if not m:
			raise Exception("Unable to parse sql datatype")
		name = m.group(1)
		params = [] if m.group(2) is None else [p.strip() for p in m.group(2)[1:-1].split(",")]
		nullable = m.group(3) is None
		return cls(name, nullable, params)

	def to_sql_str(self):
		res = self.name
		if len(self.params) > 0:
			res += "("
			res += ", ".join(self.params[:-1])
			if len(self.params) > 1:
				res += ", "
			res += self.params[-1] + ")"
		if not self.nullable:
			res += " not null"
		return res

	def to_dict(self):
		return {
			"name": self.name,
			"nullable": self.nullable,
			"params": self.params
		}

	@classmethod
	def from_dict(cls, in_d):
		return cls(**in_d)

	def serialize(self):
		res_d = self.to_dict()
		return json.dumps(res_d, indent=2)

	@classmethod
	def deserialize(cls, in_str):
		res_d = json.loads(in_str)
		return cls.from_dict(res_d)


class Column(object):
	def __init__(self, col_id, name, datatype):
		self.col_id = col_id
		self.name = name
		self.datatype = datatype

	def __repr__(self):
		return "Column(col_id=%r,name=%r,datatype=%r)" % (self.col_id, self.name, self.datatype)

	def to_dict(self):
		return {
			"col_id": self.col_id,
			"name": self.name,
			"datatype": self.datatype.to_dict()
		}

	@classmethod
	def from_dict(cls, in_d):
		res = cls(**in_d)
		res.datatype = DataType.from_dict(res.datatype)
		return res

	def serialize(self):
		res_d = self.to_dict()
		return json.dumps(res_d, indent=2)

	@classmethod
	def deserialize(cls, in_str):
		res_d = json.loads(in_str)
		return cls.from_dict(res_d)


class ExpressionNode(object):
	def __init__(self, p_id, p_name, cols_in, cols_in_consumed, cols_out, operator_info, details, pattern_signature,
				 parents=None, children=None):
		self.p_id = p_id
		self.p_name = p_name
		self.cols_in = cols_in
		self.cols_in_consumed = cols_in_consumed
		self.cols_out = cols_out
		self.operator_info = operator_info
		self.details = details
		self.pattern_signature = pattern_signature
		# NOTE: None as default value instead of set() is needed because:
		# https://stackoverflow.com/questions/4841782/python-constructor-and-default-value
		self.parents = parents if parents is not None else set()
		self.children = children if children is not None else set()

	def __repr__(self):
		return "ExpressionNode(p_id=%r,p_name=%r,cols_in=%r,cols_in_consumed=%r,cols_out=%r,operator_info=%r,details=%r,pattern_signature=%r,parents=%r,children=%r)" % (self.p_id, self.p_name, self.cols_in, self.cols_in_consumed, self.cols_out, self.operator_info, self.details, self.pattern_signature, self.parents, self.children)

	def repr_short(self):
		return "p_id=%r,cols_in=%s,cols_out=%s" % (
				self.p_id, 
				[c.col_id for c in self.cols_in], 
				[c.col_id for c in self.cols_out])

	def to_dict(self):
		return {
			"p_id": self.p_id,
			"p_name": self.p_name,
			"cols_in": [c.to_dict() for c in self.cols_in],
			"cols_in_consumed": [c.to_dict() for c in self.cols_in_consumed],
			"cols_out": [c.to_dict() for c in self.cols_out],
			"operator_info": self.operator_info,
			"details": self.details,
			"pattern_signature": self.pattern_signature,
			"parents": list(self.parents),
			"children": list(self.children)
		}

	@classmethod
	def from_dict(cls, in_d):
		res = cls(**in_d)
		res.cols_in = [Column.from_dict(c) for c in res.cols_in]
		res.cols_in_consumed = [Column.from_dict(c) for c in res.cols_in_consumed]
		res.cols_out = [Column.from_dict(c) for c in res.cols_out]
		res.parents = set(res.parents)
		res.children = set(res.children)
		return res

	def serialize(self):
		res_d = self.to_dict()
		return json.dumps(res_d, indent=2)

	@classmethod
	def deserialize(cls, in_str):
		res_d = json.loads(in_str)
		return cls.from_dict(res_d)


class CompressionNode(ExpressionNode):
	def __init__(self, p_id, p_name, cols_in, cols_in_consumed, cols_out, operator_info, details, pattern_signature,
				 cols_ex,
				 parents=None, children=None):
		ExpressionNode.__init__(self, p_id, p_name, cols_in, cols_in_consumed, cols_out, operator_info, details, pattern_signature,
								parents, children)
		self.cols_ex = cols_ex

	def __repr__(self):
		return "ExpressionNode(p_id=%r,p_name=%r,cols_in=%r,cols_in_consumed=%r,cols_out=%r,cols_ex=%r,operator_info=%r,details=%r,pattern_signature=%r,parents=%r,children=%r)" % (self.p_id, self.p_name, self.cols_in, self.cols_in_consumed, self.cols_out, self.cols_ex, self.operator_info, self.details, self.pattern_signature, self.parents, self.children)

	def to_dict(self):
		out_d = ExpressionNode.to_dict(self)
		out_d["cols_ex"] = [c.to_dict() for c in self.cols_ex]
		return out_d

	@classmethod
	def from_dict(cls, in_d):
		res = cls(**in_d)
		res.cols_in = [Column.from_dict(c) for c in res.cols_in]
		res.cols_in_consumed = [Column.from_dict(c) for c in res.cols_in_consumed]
		res.cols_out = [Column.from_dict(c) for c in res.cols_out]
		res.cols_ex = [Column.from_dict(c) for c in res.cols_ex]
		res.parents = set(res.parents)
		res.children = set(res.children)
		return res


class DecompressionNode(ExpressionNode):
	def __init__(self, *args, **kwargs):
		ExpressionNode.__init__(self, *args, **kwargs)


class OutputColumnManager(object):
	@classmethod
	def get_output_col_id(cls, in_col_id, pd_id, p_id, out_col_idx):
		return "{}__{}_{}_{}".format(in_col_id, pd_id, p_id, out_col_idx)

	@classmethod
	def get_output_col_name(cls, in_col_name, pd_id, p_id, out_col_idx):
		return "{}__{}_{}_{}".format(in_col_name, pd_id, p_id, out_col_idx)

	@classmethod
	def get_exception_col_id(cls, in_col_id):
		return str(in_col_id) + "__ex"

	@classmethod
	def get_exception_col_name(cls, in_col_name):
		return str(in_col_name) + "__ex"

	@classmethod
	def get_exception_col(cls, in_col):
		ecol_col_id = cls.get_exception_col_id(in_col.col_id)
		ecol_name = cls.get_exception_col_name(in_col.name)
		ecol_datatype = deepcopy(in_col.datatype)
		ecol_datatype.nullable = True
		return Column(ecol_col_id, ecol_name, ecol_datatype)

	@classmethod
	def is_exception_col(cls, col):
		return col.col_id.endswith("__ex")

	@classmethod
	def is_exception_col_id(cls, col_id):
		return col_id.endswith("__ex")

	@classmethod
	def get_input_col_id(cls, ex_col_id):
		if not cls.is_exception_col_id(ex_col_id):
			raise Exception("Not an exception column")
		return ex_col_id[:-len("__ex")]


def parse_schema_file(schema_file):
	schema = {}
	regex_col = re.compile(r'^"(.*?)" (.*?),?$')

	with open(schema_file, 'r') as f:
		# ignore create table line
		f.readline()
		cols = list(map(lambda c: c.strip(), f.readlines()[:-1]))

	for col_id, c in enumerate(cols):
		col_id = str(col_id)
		m = regex_col.match(c)
		if not m:
			raise Exception("Unable to parse schema file")
		col_name, datatype = m.group(1), m.group(2)
		schema[col_id] = {
			"col_id": col_id,
			"col_name": col_name,
			"datatype": datatype
		}

	return schema
