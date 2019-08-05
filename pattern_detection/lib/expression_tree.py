import os
import sys
import json
from copy import deepcopy
from lib.util import *
# debug
# from plot_expression_tree import plot_expression_tree
# import shutil
# DEBUG_COUNTER = 0
# def rm_rf(target):
# 	if os.path.exists(target):
# 		shutil.rmtree(target)
# def mkdir_p(target):
# 	if not os.path.exists(target):
# 		os.makedirs(target)
# end-debug


class ExpressionTree(object):
	"""
	NOTES:
	General:
	1) column rules:
		- "output_of" is [] (empty): it is an input column
		- "input_of" is [] (empty): it is an output column
		- both "input_of" and "output_of" are None: not used in any expression node in the tree (both input and output column)
	Compression:
	1) multiple expression nodes can have the same column as input; this is the case of multiple patterns on the same column
	2) a non-exception column can only be the output column of a single expression node
	3) an exception column can be the output column of multiple expression nodes (those that took the in_col as input)
	Decompression:
	1) multiple expression nodes can have the same column as output; this is the case of multiple patterns on the same column
	2) a non-exception column can only be the input column of a single expression node
	3-1) all exception columns are input columns
	3-2) no exception column is input of a decompression node
	3-3) exception columns are not considered output columns
	"""
	def __init__(self, in_columns, tree_type):
		self.levels = []
		self.nodes = {}
		""" nodes item format: node_id (str): expr_n (ExpressionNode) """
		self.columns = {}
		""" columns item format: col_id (str): {
				"col_info": col (Column),
				"output_of": node_id (str), # this column is output of node_id
				"input_of": node_id (str) # this column is input for node_id
			} """
		self.type = tree_type
		self.node_class = self.get_node_class()
		# add input columns
		for in_col in in_columns:
			self.columns[in_col.col_id] = {
				"col_info": deepcopy(in_col),
				"output_of": [],
				"input_of": []
			}

	def get_node_class(self):
		if self.type == "compression":
			return CompressionNode
		elif self.type == "decompression":
			return DecompressionNode
		else:
			raise Exception("Invalid tree_type: {}".format(self.type))

	def to_dict(self):
		res = {
			"type": self.type,
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
		# json.dumps(expr_tree_dict, indent=2)

		in_columns = [Column.from_dict(expr_tree_dict["columns"][col_id]["col_info"]) for col_id in expr_tree_dict["in_columns"]]
		expr_tree = cls(in_columns, expr_tree_dict["type"])

		for level in expr_tree_dict["levels"]:
			expr_nodes = [expr_tree.node_class.from_dict(expr_tree_dict["nodes"][node_id]) for node_id in level]
			expr_tree.add_level(expr_nodes)

		return expr_tree

	def __repr__(self):
		output = ""
		for l_idx, l_nodes in enumerate(self.levels):
			output += "\n[level={}] nodes={}".format(l_idx, l_nodes)
			for node_id in l_nodes:
				node = self.nodes[node_id]
				output += "\n" + node.repr_short()
		return output

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
					if p_node_id not in expr_n.parents:
						expr_n.parents.add(p_node_id)
					if p_node_id not in self.nodes:
						raise Exception("Inexistent parent node: p_node_id={}".format(p_node_id))
					p_node = self.nodes[p_node_id]
					p_node.children.add(node_id)

			# add output columns; fill in "output_of"
			for out_col in expr_n.cols_out:
				if out_col.col_id not in self.columns:
					self.columns[out_col.col_id] = {
						"col_info": deepcopy(out_col),
						"output_of": [],
						"input_of": []
					}
				if (len(self.columns[out_col.col_id]["output_of"]) > 0 and
					self.node_class == CompressionNode):
					raise Exception("Duplicate output column: out_col={}".format(out_col))
				self.columns[out_col.col_id]["output_of"].append(node_id)

			# if CompressionNode: add exception columns; append to "output_of"
			if self.node_class == CompressionNode:
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
		"""
		Compression:
			columns that are not consumed by any node
		Decompression:
			same as Compression, but excluding exception columns
		"""
		res = []
		for col_id, col_item in self.columns.items():
			for node_id in col_item["input_of"]:
				if col_id in {cic_col.col_id for cic_col in self.nodes[node_id].cols_in_consumed}:
					break
			else:
				if (self.node_class == CompressionNode or
					(self.node_class == DecompressionNode and
					 not OutputColumnManager.is_exception_col(col_item["col_info"]))
				   ):
					res.append(col_id)
		return sorted(res)

	def get_unused_columns(self):
		# NOTE: used but not consumed columns have len(input_of) > 0
		return sorted(list(filter(lambda col_id: len(self.columns[col_id]["output_of"]) == 0 and len(self.columns[col_id]["input_of"]) == 0, self.columns.keys())))

	def _dfs(self, node_id, visited):
		visited.add(node_id)
		yield node_id
		for child_id in self.nodes[node_id].children:
			if child_id in visited:
				continue
			yield from self._dfs(child_id, visited)

	def get_connected_components(self, debug=False):
		"""
		Returns: List[ExpressionTree]
		"""
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
			for n_node_id in col["input_of"][1:]:
				n_c_id = _get_component_id(n_node_id)
				if n_c_id is None:
					raise Exception("No component for n_node_id={}".format(n_node_id))
				if component_id != n_c_id:
					connected_components[component_id] |= connected_components[n_c_id]
					del connected_components[n_c_id]

		if debug:
			print(self.get_in_columns(), connected_components)
			print(json.dumps(self.to_dict(), indent=2))

		# create an expression tree for each connected component
		res = []
		for cc in connected_components.values():
			# print("\ncc:", cc)
			expr_node_levels = []
			for level in self.levels:
				expr_nodes = [deepcopy(self.nodes[node_id]) for node_id in level if node_id in cc]
				if len(expr_nodes) > 0:
					expr_node_levels.append(expr_nodes)
				# print("l:", [en.p_id for en in expr_nodes])
			if len(expr_node_levels) == 0:
				raise Exception("No expression nodes in connected component")
			in_columns_unique_ids = {col.col_id for expr_node in expr_node_levels[0] for col in expr_node.cols_in}
			in_columns = [self.columns[col_id]["col_info"] for col_id in in_columns_unique_ids]
			# print([col.col_id for col in in_columns])
			expr_tree = ExpressionTree(in_columns, self.type)
			for expr_nodes in expr_node_levels:
				expr_tree.add_level(expr_nodes)
			res.append(expr_tree)

		return res

	def get_topological_order(self):
		unvisited_nodes = set(self.nodes.keys())
		explored_nodes = []

		def m_dfs(node_id):
			for child_id in self.nodes[node_id].children:
				if child_id not in unvisited_nodes:
					continue
				unvisited_nodes.remove(child_id)
				m_dfs(child_id)
			explored_nodes.append(node_id)

		while len(unvisited_nodes) > 0:
			m_dfs(unvisited_nodes.pop())

		return explored_nodes[::-1]

	@classmethod
	def merge(cls, tree_a, tree_b, tree_type="compression"):
		""" NOTE: this method assumes that either:
		- tree_a and tree_b are independent trees
		- OR tree_a and tree_b have a common subtree starting at the root
		*for each connected component
		"""
		# debug
		# global DEBUG_COUNTER
		# DEBUG_COUNTER += 1
		# end-debug

		# split into connected components
		ccs_a = tree_a.get_connected_components()
		ccs_b = tree_b.get_connected_components()
		# debug
		# debug = DEBUG_COUNTER == 16
		# ccs_a = tree_a.get_connected_components(debug=debug)
		# ccs_b = tree_b.get_connected_components()
		# end-debug
		
		# debug
		# out_dir = "/tmp/debug/{}".format(DEBUG_COUNTER)
		# rm_rf(out_dir)
		# mkdir_p(out_dir)
		# out_file = out_dir+"/a.svg"
		# plot_expression_tree(tree_a, out_file)
		# out_file = out_dir+"/b.svg"
		# plot_expression_tree(tree_b, out_file)
		# for d_cc_idx, d_cc in enumerate(ccs_a):
		# 	out_file =  out_dir+"/a_{}.svg".format(d_cc_idx)
		# 	plot_expression_tree(d_cc, out_file)
		# for d_cc_idx, d_cc in enumerate(ccs_b):
		# 	out_file =  out_dir+"/b_{}.svg".format(d_cc_idx)
		# 	plot_expression_tree(d_cc, out_file)
		# end-debug

		# find connected components that need to be merged
		merge_pairs, merge_ccs_a, merge_ccs_b = [], set(), set()
		for idx_a, cc_a in enumerate(ccs_a):
			for idx_b, cc_b in enumerate(ccs_b):
				if len(set(cc_a.columns.keys()) & cc_b.columns.keys()) > 0:
					if idx_a in merge_ccs_a or idx_b in merge_ccs_b:
						# debug
						# print("DEBUG_COUNTER={}".format(DEBUG_COUNTER))
						# print("merge_pairs: {}".format(merge_pairs))
						# print("conflict: idx_a={}, idx_b={}".format(idx_a, idx_b))
						# end-debug
						print("error: multiple merge candidates for the same cc")
						raise Exception("Unable to merge trees")
					merge_pairs.append((idx_a, idx_b))
					merge_ccs_a.add(idx_a)
					merge_ccs_b.add(idx_b)

		ccs = []
		# add all ccs that do not need to be merged to the cc list
		for idx_a, cc_a in enumerate(ccs_a):
			if idx_a not in merge_ccs_a:
				ccs.append(cc_a)
		for idx_b, cc_b in enumerate(ccs_b):
			if idx_b not in merge_ccs_b:
				ccs.append(cc_b)

		# merge connected components that need to be merged
		for (idx_a, idx_b) in merge_pairs:
			cc_merged = ExpressionTree._merge_ccs(ccs_a[idx_a], ccs_b[idx_b], tree_type)
			ccs.append(cc_merged)

		# put all connected components together in a single ExpressionTree
		tree_res = ExpressionTree._unify_ccs(ccs, tree_type)

		# add unused columns
		for tree_tmp in [tree_a, tree_b]:
			for col_id in tree_tmp.get_unused_columns():
				if col_id not in tree_res.columns:
					tree_res.columns[col_id] = tree_tmp.columns[col_id]

		# debug
		# out_file =  out_dir+"/ab.svg"
		# plot_expression_tree(tree_res, out_file)
		# end-debug

		return tree_res

	@classmethod
	def _merge_ccs(cls, cc_a, cc_b, tree_type):
		in_columns_a, in_columns_b = cc_a.get_in_columns(), cc_b.get_in_columns()
		if set(in_columns_a) != set(in_columns_b):
			raise Exception("cc_a and cc_b have different root nodes")

		in_columns = [cc_a.get_column(col_id)["col_info"] for col_id in cc_a.get_in_columns()]
		tree_res = ExpressionTree(in_columns, tree_type)

		levels_a, levels_b = cc_a.get_node_levels(), cc_b.get_node_levels()
		if len(levels_b) > len(levels_a):
			cc_a, cc_b = cc_b, cc_a
			levels_a, levels_b = levels_b, levels_a
		
		l_b = 0
		for node_ids_b in levels_b:
			expr_nodes_a = [cc_a.get_node(node_id) for node_id in levels_a[l_b]]
			expr_nodes_b = [cc_b.get_node(node_id) for node_id in node_ids_b]
			l_b += 1

			expr_nodes_dict = {}
			for expr_node in expr_nodes_a + expr_nodes_b:
				key = (expr_node.p_name, 
					   expr_node.pattern_signature,
					   ",".join(sorted([c.col_id for c in expr_node.cols_in])),
					   ",".join(sorted([c.col_id for c in expr_node.cols_out])))
				# keep only unique nodes
				expr_nodes_dict[key] = expr_node 
			
			expr_nodes_merged = expr_nodes_dict.values()
			tree_res.add_level(expr_nodes_merged)

		for l_a in range(l_b, len(levels_a)):
			expr_nodes_a = [cc_a.get_node(node_id) for node_id in levels_a[l_a]]
			tree_res.add_level(expr_nodes_a)

		return tree_res

	@classmethod
	def _unify_ccs(cls, ccs, tree_type):
		in_columns = []
		for cc in ccs:
			in_columns_cc = [cc.get_column(col_id)["col_info"] for col_id in cc.get_in_columns()]
			in_columns.extend(in_columns_cc)
		tree_res = ExpressionTree(in_columns, tree_type)

		# add levels
		nb_levels = 0
		while True:
			expr_nodes = []
			for cc in ccs:
				levels_cc = cc.get_node_levels()
				if (nb_levels + 1) > len(levels_cc):
					continue
				expr_nodes_cc = [cc.get_node(node_id) for node_id in levels_cc[nb_levels]]
				expr_nodes.extend(expr_nodes_cc)
			if len(expr_nodes) == 0:
				break
			tree_res.add_level(expr_nodes)
			nb_levels += 1

		return tree_res


def read_expr_tree(expr_tree_file):
	with open(expr_tree_file, 'r') as f:
		expr_tree_dict = json.load(f)
		return ExpressionTree.from_dict(expr_tree_dict)
