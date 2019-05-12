import os
import sys
import json
from copy import deepcopy
from lib.util import *


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

	def to_dict(self):
		res = {
			"levels": deepcopy(self.levels),
			"nodes": {},
			"columns": {},
			"in_columns": self.get_in_columns()
		}
		for node_id, expr_n in self.nodes.items():
			res["nodes"][node_id] = expr_n.to_dict()
		for col_id, col_item in self.columns.items():
			res["columns"][col_id] = {
				"col_info": col_item["col_info"].to_dict(),
				"output_of": col_item["output_of"],
				"input_of": col_item["input_of"]
			}
		return res

	@classmethod
	def from_dict(cls, expr_tree_dict):
		json.dumps(expr_tree_dict, indent=2)

		in_columns = [Column.from_dict(expr_tree_dict["columns"][col_id]["col_info"]) for col_id in expr_tree_dict["in_columns"]]
		expr_tree = cls(in_columns)

		for level in expr_tree_dict["levels"]:
			expr_nodes = [ExpressionNode.from_dict(expr_tree_dict["nodes"][node_id]) for node_id in level]
			expr_tree.add_level(expr_nodes)

		return expr_tree

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

	def get_in_columns(self):
		return sorted(list(filter(lambda col_id: len(self.columns[col_id]["output_of"]) == 0, self.columns.keys())))

	def get_out_columns(self):
		return sorted(list(filter(lambda col_id: len(self.columns[col_id]["input_of"]) == 0, self.columns.keys())))

	def get_unused_columns(self):
		return sorted(list(filter(lambda col_id: len(self.columns[col_id]["output_of"]) == 0 and len(self.columns[col_id]["input_of"]) == 0, self.columns.keys())))

	def _dfs(self, node_id, visited):
		visited.add(node_id)
		yield node_id
		for child_id in self.nodes[node_id].children:
			if child_id in visited:
				continue
			yield from self._dfs(child_id, visited)

	def get_connected_components(self):
		unused_nodes = set(self.nodes.keys())
		connected_components = {}

		def _get_component_id(node_id):
			for c_id, component in connected_components.items():
				if node_id in component:
					return c_id
			return None

		# unify expr_nodes based on children property
		cnt = 0
		while len(unused_nodes) > 0:
			cnt += 1
			node_id = unused_nodes.pop()
			component = set()

			for n_node_id in self._dfs(node_id, set()):
				n_c_id = _get_component_id(n_node_id)
				if n_c_id is not None:
					component = component.union(connected_components[n_c_id])
					del connected_components[n_c_id]
				else:
					component.add(n_node_id)
					unused_nodes.discard(n_node_id)
			connected_components[cnt] = component

		# merge first level expr_nodes that have common input columns
		for col_id in self.get_in_columns():
			col = self.columns[col_id]
			if len(col["input_of"]) < 2:
				continue
			node_id = col["input_of"][0]
			component_id = _get_component_id(node_id)
			if component_id is None:
				raise Exception("No component for node_id={}".format(node_id))
			component = connected_components[component_id]
			for n_node_id in col["input_of"][1:]:
				n_c_id = _get_component_id(n_node_id)
				if n_c_id is None:
					raise Exception("No component for n_node_id={}".format(n_node_id))
				component = component.union(connected_components[n_c_id])
				del connected_components[n_c_id]
			connected_components[component_id] = component

		# TODO: return List[ExpressionTree]

		return connected_components


def read_expr_tree(expr_tree_file):
	with open(expr_tree_file, 'r') as f:
		expr_tree_dict = json.load(f)
		return ExpressionTree.from_dict(expr_tree_dict)
