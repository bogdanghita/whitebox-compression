import os
import sys
import json
import re


class FileDriver(object):
	def __init__(self, fd, args):
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
	def __init__(self, p_id, cols_in, cols_out, operator_info, details):
		self.p_id = p_id
		self.cols_in = cols_in
		self.cols_out = cols_out
		self.operator_info = operator_info
		self.details = details

	def to_dict(self):
		return {
			"p_id": self.p_id,
			"cols_in": [c.to_dict() for c in self.cols_in],
			"cols_out": [c.to_dict() for c in self.cols_out],
			"operator_info": self.operator_info,
			"details": self.details
		}

	@classmethod
	def from_dict(cls, in_d):
		res = cls(**in_d)
		res.cols_in = [Column.from_dict(c) for c in res.cols_in]
		res.cols_out = [Column.from_dict(c) for c in res.cols_out]
		return res

	def serialize(self):
		res_d = self.to_dict()
		return json.dumps(res_d, indent=2)

	@classmethod
	def deserialize(cls, in_str):
		res_d = json.loads(in_str)
		return cls.from_dict(res_d)


def to_row_mask(selected_rows, nb_rows_total):
	row_mask = ['0'] * nb_rows_total
	try:
		for r in selected_rows:
			row_mask[r] = '1'
	except Exception as e:
		print("r={}, nb_rows_total={}".format(r, nb_rows_total))
		raise e
	return "".join(row_mask)
