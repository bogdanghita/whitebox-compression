import os
import sys
from copy import deepcopy
from lib.util import ExpressionNode


class ExpressionTree(object):
	"""
	NOTES:
	1) multiple expression nodes can have the same column as input; this is the case of multiple patterns on the same column
	2) a non-exception column can only be the output column of a single expression node
	3) an exception column can be the output columns of multiple expression nodes (those that took the in_col as input)
	4) column rules:
		- "output_of" is [] (empty): it is an input column
		- "input_of" is [] (empty): it is an output column
		- both "input_of" and "output_of" are None: not used in any expression node in the tree (both input and output column)
	"""
	def __init__(self, in_columns):
		self.levels = []
		self.nodes = {}
		""" nodes item format: node_id (str): expr_n (ExpressionNode) """
		self.columns = {}
		""" columns item format: col_id (str): {
				"col_info": col (Column),
				"output_of": node_id (str), # this column is output of node_id
				"input_of": node_id (str) # this column is input for node_id
			} """
		# add input columns
		for in_col in in_columns:
			self.columns[in_col.col_id] = {
				"col_info": deepcopy(in_col),
				"output_of": [],
				"input_of": []
			}

	def add_level(self, expr_nodes):
		level = []

		for idx, expr_n in enumerate(expr_nodes):
			node_id = "{}_{}".format(len(self.levels), idx)
			if node_id in self.nodes:
				raise Exception("Duplicate expression node: node_id={}".format(node_id))

			# add expression node
			self.nodes[node_id] = expr_n
			level.append(node_id)

			# validate input columns; add parent & child nodes; fill in "input_of"
			for in_col in expr_n.cols_in:
				if in_col.col_id not in self.columns:
					raise Exception("Invalid input column: in_col={}".format(in_col))
				col_item = self.columns[in_col.col_id]
				# fill in "input_of"
				col_item["input_of"].append(node_id)
				# add parent & child nodes
				for p_node_id in col_item["output_of"]:
					# check if not already added
					if p_node_id in expr_n.parents:
						continue
					if p_node_id not in self.nodes:
						raise Exception("Inexistent parent node: p_node_id={}".format(p_node_id))
					p_node = self.nodes[p_node_id]
					expr_n.parents.add(p_node_id)
					p_node.children.add(node_id)

			# add output columns; fill in "output_of"
			for out_col in expr_n.cols_out:
				if out_col.col_id in self.columns:
					raise Exception("Duplicate output column: out_col={}".format(out_col))
				self.columns[out_col.col_id] = {
					"col_info": deepcopy(out_col),
					"output_of": [node_id],
					"input_of": []
				}

			# add exception columns; append to "output_of"
			for ex_col in expr_n.cols_ex:
				if ex_col.col_id not in self.columns:
					self.columns[ex_col.col_id] = {
						"col_info": deepcopy(ex_col),
						"output_of": [],
						"input_of": []
					}
				self.columns[ex_col.col_id]["output_of"].append(node_id)

		# add new level
		self.levels.append(level)

	def get_node(self, node_id):
		if node_id not in self.nodes:
			return None
		return self.nodes[node_id]

	def get_node_levels(self):
		return self.levels

	def get_column(self, col_id):
		if col_id not in self.columns:
			return None
		return self.columns[col_id]

	# def _get_column_parents(self, col_id):
	# 	# NOTE: this method is not tested
	# 	col = self.get_column(col_id)
	# 	if col is None:
	# 		raise Exception("Invalid column: col_id={}".format(col_id))
	# 	# NOTE: the line below is not valid anymore, since output_of is a list (possible multiple parents for exception columns)
	# 	parent_node = col["output_of"]
	# 	if parent_node is None:
	# 		return []
	# 	return [c.col_id for c in parent_node.cols_in]
	#
	# def get_column_ancestors(self, col_id):
	# 	# NOTE: this method is not tested
	# 	ancestors = []
	# 	q = [col_id]
	# 	while len(q) > 0:
	# 		col_id = q.pop(0)
	# 		parents = self._get_column_parents(col_id)
	# 		print("parents={}".format(parents))
	# 		q.extend(parents)
	# 		ancestors.extend(parents)
	# 	return ancestors

	def get_in_columns(self):
		return sorted(list(filter(lambda col_id: len(self.columns[col_id]["output_of"]) == 0, self.columns.keys())))

	def get_out_columns(self):
		return sorted(list(filter(lambda col_id: len(self.columns[col_id]["input_of"]) == 0, self.columns.keys())))

	def get_unused_columns(self):
		return sorted(list(filter(lambda col_id: len(self.columns[col_id]["output_of"]) == 0 and len(self.columns[col_id]["input_of"]) == 0, self.columns.keys())))
