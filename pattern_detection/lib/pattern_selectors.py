import os
import sys
import itertools
from copy import deepcopy
from bitstring import *
from overrides import overrides
from lib.util import *
from patterns import *


class PatternSelector(object):
	def __init__(self):
		pass

	def select_patterns(self, patterns, columns, nb_rows):
		""" Selects the best (combination of) pattern(s) for the given columns

		Returns:
			expression_nodes: list(
				dict(
					p_id: # id of the pattern
					cols_in: list(col_id1, col_id2, ...) # list of input columns; type: util.Column
					cols_out: [rcol_1, rcol_2, ...], # list of resulting columns; type: util.Column
					cols_ex: [rcol_1, rcol_2, ...], # list of exception columns; type: util.Column
					operator_info: dict() # operator parameters (used when applying the transformation)
					details: dict() # other details
				)
			)

		NOTE-1: a column can appear in multiple expression_nodes, each being applied only on a subset of its rows;
				the key point here is that the subsets of rows will not overlap
		TODO-1: take care to satisfy the above property
		TODO-2: See rules for pattern selection in:
				notes-week_16/Reconstruction/decompression/Rules

		NOTE-2: some columns might not appear in any of the expression_nodes;
				this means that they should not be processed with any of the given patterns
		"""
		raise Exception("Not implemented")


class DummyPatternSelector(PatternSelector):
	"""
	TODO: describe the strategy used by this PatternSelector

	NOTE: this PatternSelector does not work with operators that take more than one column as input (e.g. correlated columns)
	"""

	def __init__(self, min_col_coverage):
		PatternSelector.__init__(self)
		self.min_col_coverage = min_col_coverage

	@overrides
	def select_patterns(self, patterns, columns, nb_rows):
		expression_nodes = []

		for col in columns:
			# NOTE: each pattern detector also outputs nulls; TODO: keep only one "nulls" per column

			col_patterns = []
			for p_name, p_columns in patterns.items():
				if col.col_id not in p_columns["columns"]:
					continue
				col_patterns.extend(p_columns["columns"][col.col_id])

			# choose the best pattern with coverage higher than MIN_COVERAGE
			max_coverage, best_p = -1, None
			for col_p in col_patterns:
				if col_p["coverage"] < self.min_col_coverage:
					continue
				if col_p["coverage"] > max_coverage:
					max_coverage, best_p = col_p["coverage"], col_p
			if best_p is None:
				# print("debug: no pattern selected for column={}".format(col))
				# TODO: maybe do something here
				pass
			else:
				pd = get_pattern_detector(col_p["p_id"])
				expr_n = pd.get_compression_node(col_p)
				expression_nodes.append(expr_n)

		return expression_nodes


class CoveragePatternSelector(PatternSelector):
	"""
	Selects the best combination of patterns for each column
	- each combination has a score:
		- combined coverage of all its patterns
	- patterns are considered only if their coverage is higher than MIN_COVERAGE
	- if number of considered patterns is:
		< max_candidate_patterns_exhaustive:
		- exhaustive approach (brute force)
		else:
		- greedy approach

	TODO-1: when computing the score also take into account:
	- overlap (smaller is better)
	TODO-2: create a new pattern selector the computes the score based on other metrics:
	- see notes-week_13 on Drive

	NOTE: this PatternSelector does not work with operators that take more than one column as input (e.g. correlated columns)
	"""

	def __init__(self, min_col_coverage, max_candidate_patterns_exhaustive=10):
		PatternSelector.__init__(self)
		self.min_col_coverage = min_col_coverage
		self.max_candidate_patterns_exhaustive = max_candidate_patterns_exhaustive

	def _select_patterns_exhaustive(self, candidate_patterns, nb_rows):
		candidate_patterns_idxs = [i for i in range(0, len(candidate_patterns))]
		row_mask_list = []
		for col_p in candidate_patterns:
			# print(col_p)
			row_mask = BitArray(length=nb_rows)
			for r in col_p["rows"]:
				row_mask[r] = True
			row_mask_list.append(row_mask)

		best_score, best_res = -float("inf"), None
		for k in range(1, len(candidate_patterns)+1):
			for res in itertools.combinations(candidate_patterns_idxs, k):
				coverage_mask = BitArray(length=nb_rows)
				for i in res:
					coverage_mask |= row_mask_list[i]
				coverage = coverage_mask.count(1)
				score = coverage
				if score > best_score:
					best_score = score
				best_res = [candidate_patterns[i] for i in res]
		return best_res

	def _select_patterns_greedy(self, candidate_patterns, nb_rows):
		# TODO: implement a basic greedy approach
		raise Exception("Not implemented")

	@overrides
	def select_patterns(self, patterns, columns, nb_rows):
		expression_nodes = []

		for col in columns:
			# NOTE: each pattern detector also outputs nulls; TODO: keep only one "nulls" per column

			col_patterns = []
			for p_name, p_columns in patterns.items():
				if col.col_id not in p_columns["columns"]:
					continue
				col_patterns.extend(p_columns["columns"][col.col_id])

			# debug
			# print("\n[{}]".format(col.col_id))
			# for col_p in col_patterns:
			# 	print("{:.2f} {}".format(col_p["coverage"], col_p["p_id"]))
			# end-debug

			# select all patterns with coverage higher than MIN_COVERAGE as candidates
			candidate_patterns = []
			for col_p in col_patterns:
				if col_p["coverage"] < self.min_col_coverage:
					continue
				candidate_patterns.append(col_p)
			if len(candidate_patterns) == 0:
				# print("debug: no pattern selected for column={}".format(col))
				# TODO: maybe do something here
				continue

			# select the patterns that when put toghether give: best coverage, min_overlap
			if len(candidate_patterns) < self.max_candidate_patterns_exhaustive:
				selected_patterns = self._select_patterns_exhaustive(candidate_patterns, nb_rows)
			else:
				print("debug: falling back to greedy pattern selection; len(candidate_patterns)={}, col_id={}".format(len(candidate_patterns), col.col_id))
				selected_patterns = self._select_patterns_greedy(candidate_patterns, nb_rows)

			for col_p in selected_patterns:
				details = deepcopy(col_p["details"])
				details.update({
					"coverage": col_p["coverage"],
					"null_coverage": col_p["null_coverage"]
				})
				pd = get_pattern_detector(col_p["p_id"])
				expr_n = pd.get_compression_node(col_p)
				expression_nodes.append(expr_n)

		return expression_nodes


class PriorityPatternSelector(PatternSelector):
	"""
	Selects patterns based on their priority; applies CoveragePatternSelector for patterns with the same priority

	NOTE: a column can only be used (or shared between shared) by patterns with the same priority
		  e.g. a column cannot be used by a pd with priority 1 and a pd with priority 2; but it can be used by 2 pds with priority 1
	"""

	def __init__(self, priorities, coverage_pattern_selector_args):
		"""
		Params:
			priorities: List[List]
				Outer list: groups of pattern detectors; first group has the highest priority, last has the lowest priority
				Inner list: pattern detector names with the same priority
			coverage_pattern_selector_args: list or dict with arguments for the CoveragePatternSelector
		"""
		PatternSelector.__init__(self)
		self.priorities = priorities

		# init CoveragePatternSelector instance
		if isinstance(coverage_pattern_selector_args, list):
			self.coverage_ps = CoveragePatternSelector(*coverage_pattern_selector_args)
		elif isinstance(coverage_pattern_selector_args, dict):
			self.coverage_ps = CoveragePatternSelector(**coverage_pattern_selector_args)
		else:
			raise Exception("Invalid coverage_pattern_selector_args")

	def update_remaining_patterns(self, patterns, expression_nodes):
		""" Remove patterns that take as input already used columns """
		used_columns = {col.col_id for expr_n in expression_nodes for col in expr_n.cols_in}

		# remove from patterns
		for p_name, p_item in patterns.items():
			for col_id in list(p_item["columns"].keys()):
				col_p_list = p_item["columns"][col_id]
				# delete patterns that have the main col in used_columns
				if col_id in used_columns:
					del p_item["columns"][col_id]
					continue
				# delete patterns that have a secondary column in used_columns (e.g. source_column in ColumnCorrelation)
				for i in range(len(col_p_list)):
					col_p_item = col_p_list[i]
					if len(used_columns & {c.col_id for c in col_p_item["in_columns"]}) > 0:
						del col_p_list[i]

	@overrides
	def select_patterns(self, patterns, columns, nb_rows):
		patterns, columns = deepcopy(patterns), deepcopy(columns)
		expression_nodes = []

		for pattern_group in self.priorities:
			tmp_patterns = {p_name: patterns[p_name] for p_name in patterns.keys() if p_name in pattern_group}
			tmp_expression_nodes = self.coverage_ps.select_patterns(tmp_patterns, columns, nb_rows)
			expression_nodes.extend(tmp_expression_nodes)
			self.update_remaining_patterns(patterns, tmp_expression_nodes)

		return expression_nodes


class CorrelationPatternSelector(PatternSelector):
	"""
	Specialized pattern selector for ColumnCorrelation

	NOTE: does not support any other pattern except ColumnCorrelation
	"""

	def __init__(self):
		PatternSelector.__init__(self)

	def get_connected_components(self, corrs):
		"""
		NOTE: We need the connected components considering the graph as undirected (even though it is directed)
		"""

		nodes = {node_id for edge in corrs for node_id in edge[:2]}
		ccs = {idx:{node_id} for idx, node_id in enumerate(nodes)}

		def get_cc(node_id):
			for cc, node_ids in ccs.items():
				if node_id in node_ids:
					return cc
			raise Exception("node_id not found in any cc: node_id={}".format(node_id))

		for edge in corrs:
			src, dst = edge[0], edge[1]
			src_cc, dst_cc = get_cc(src), get_cc(dst)
			if src_cc == dst_cc:
				continue
			ccs[src_cc] |= ccs[dst_cc]
			del ccs[dst_cc]

		res = {cc:[] for cc in ccs.keys()}
		for edge in corrs:
			cc = get_cc(edge[0])
			res[cc].append(edge)

		return list(res.values())

	def select_patterns_cc(self, corr_cc, p_item, columns, nb_rows):
		# build adjacency list
		nodes = {node_id: {"in": set(), "out": set()} for corr in corr_cc for node_id in corr[:2]}
		for corr in corr_cc:
			src, dst = corr[0], corr[1]
			nodes[src]["out"].add(dst)
			nodes[dst]["in"].add(src)

		def get_corr(src, dst):
			for corr in corr_cc:
				if corr[0] == src and corr[1] == dst:
					return corr
			raise Exception("Invalid (src, dst) pair: src={}, dst={}".format(src, dst))

		def get_node_score(node_id):
			in_d = len(nodes[node_id]["in"])
			out_d = len(nodes[node_id]["out"])
			min_p_in_d = min([len(nodes[p_id]["in"]) for p_id in nodes[node_id]["in"]])
			return (out_d, min_p_in_d, in_d)

		def get_parent_score(parent_node_id, node_id):
			in_d = len(nodes[parent_node_id]["in"])
			out_d = len(nodes[parent_node_id]["out"])
			corr_coef = get_corr(parent_node_id, node_id)[2]
			# print(parent_node_id, node_id, ":", in_d, out_d, -corr_coef)
			return (in_d, -corr_coef, -out_d)

		def get_next_corr():
			# select from nodes that have at least one "in" node
			candidate_nodes = [node_id for node_id in nodes if len(nodes[node_id]["in"]) > 0]
			if len(candidate_nodes) == 0:
				return None
			dst = min(candidate_nodes, key=get_node_score)
			src = min(nodes[dst]["in"], key=lambda node_id: get_parent_score(node_id, dst))
			return get_corr(src, dst)

		def update_nodes(corr):
			src, dst = corr[0], corr[1]
			# remove (_, dst) edges
			for p_node in nodes[dst]["in"]:
				if p_node != src:
					nodes[p_node]["out"].remove(dst)
			# remove (dst, _) edges
			for p_node in nodes[dst]["out"]:
				nodes[p_node]["in"].remove(dst)
			# remove dst
			del nodes[dst]
			""" remove (_, src) edges
			NOTE: for now, a node that determines another node cannot be determined;
			see notes for more info """
			for p_node in nodes[src]["in"]:
				nodes[p_node]["out"].remove(src)
			nodes[src]["in"].clear()

		def get_expression_node(corr):
			source_col_id, target_col_id = corr[0], corr[1]
			if target_col_id not in p_item["columns"]:
				raise Exception("Invalid target_col_id: {}".format(target_col_id))
			col_p_list = p_item["columns"][target_col_id]
			for col_p in col_p_list:
				if col_p["details"]["src_col_id"] == source_col_id:
					break
			else:
				raise Exception("Invalid source_col_id: {}".format(source_col_id))
			pd = get_pattern_detector(col_p["p_id"])
			expr_n = pd.get_compression_node(col_p)
			return expr_n

		expression_nodes = []
		while True:
			corr = get_next_corr()
			if corr is None:
				break
			# print("selected corr: {}".format(corr))
			update_nodes(corr)
			expr_n = get_expression_node(corr)
			expression_nodes.append(expr_n)

		return expression_nodes

	def select_patterns(self, patterns, columns, nb_rows):
		if ColumnCorrelation.get_p_name() not in patterns:
			return []
		p_item = patterns[ColumnCorrelation.get_p_name()]

		corrs = [(col_p_item["details"]["src_col_id"], col_id, col_p_item["details"]["corr_coef"]) for col_id, col_p_list in p_item["columns"].items() for col_p_item in col_p_list]
		# print(corrs)

		corr_cc_list = self.get_connected_components(corrs)
		# print(corr_cc_list)

		expression_nodes = []
		for corr_cc in corr_cc_list:
			tmp_expression_nodes = self.select_patterns_cc(corr_cc, p_item, columns, nb_rows)
			expression_nodes.extend(tmp_expression_nodes)

		return expression_nodes


'''
================================================================================
'''
ps_list = [
DummyPatternSelector,
CoveragePatternSelector,
PriorityPatternSelector,
CorrelationPatternSelector,
]
ps_map = {ps.__name__.lower(): ps for ps in ps_list}

def get_pattern_selector(ps_name):
	default_exception = Exception("Invalid pattern selector name: {}".format(ps_name))

	ps_name = ps_name.lower()
	if ps_name in ps_map:
		return ps_map[ps_name]

	raise default_exception
