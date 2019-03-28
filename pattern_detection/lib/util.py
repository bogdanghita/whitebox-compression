import os
import sys


class Column(object):
	def __init__(self, col_id, name, datatype):
		self.col_id = col_id
		self.name = name
		self.datatype = datatype

	def __repr__(self):
		return "Column(col_id=%r,name=%r,datatype=%r)" % (self.col_id, self.name, self.datatype)
