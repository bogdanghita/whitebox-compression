import os
import sys


class Column(object):
	def __init__(self, col_id, name, datatype):
		self.col_id = col_id
		self.name = name
		self.datatype = datatype

	def __repr__(self):
		return "Column(col_id=%r,name=%r,datatype=%r)" % (self.col_id, self.name, self.datatype)


class ExpressionNode(object):
	def __init__(self, p_id, cols_in, cols_out, operator_info, details):
		self.p_id = p_id
		self.cols_in = cols_in
		self.cols_out = cols_out
		self.operator_info = operator_info
		self.details = details

	def serialize(self):
		# TODO
		return "<not-implemented>"

	@staticmethod
	def deserialize(expr_node):
		# TODO
		return "<not-implemented>"


def to_row_mask(selected_rows, nb_rows_total):
	row_mask = ['0'] * nb_rows_total
	try:
		for r in selected_rows:
			row_mask[r] = '1'
	except Exception as e:
		print("r={}, nb_rows_total={}".format(r, nb_rows_total))
		raise e
	return "".join(row_mask)
