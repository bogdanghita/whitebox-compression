import os
import sys
import itertools
from bitstring import *
from lib.util import ExpressionNode


class PatternSelector(object):
	@classmethod
	def select_patterns(cls, patterns, columns, nb_rows):
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

		NOTE-2: some columns might not appear in any of the expression_nodes;
				this means that they should not be processed with any of the given patterns
		"""
		raise Exception("Not implemented")


class DummyPatternSelector(PatternSelector):
	"""
	TODO: describe the strategy used by this PatternSelector

	NOTE: this PatternSelector does not work with operators that take more than one column as input (e.g. correlated columns)
	"""

	MIN_COVERAGE = 0.2

	@classmethod
	def select_patterns(cls, patterns, columns, nb_rows):
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
				if col_p["coverage"] < cls.MIN_COVERAGE:
					continue
				if col_p["coverage"] > max_coverage:
					max_coverage, best_p = col_p["coverage"], col_p
			if best_p is None:
				print("debug: no pattern selected for column={}".format(col))
				# TODO: maybe do something here
			else:
				exp_node = ExpressionNode(
					p_id=best_p["p_id"],
					cols_in=[col],
					cols_out=best_p["res_columns"],
					cols_ex=best_p["ex_columns"],
					operator_info=best_p["operator_info"],
					details={
						"coverage": best_p["coverage"],
						"null_coverage": best_p["details"]["null_coverage"]
					},
					pattern_signature=best_p["pattern_signature"])
				expression_nodes.append(exp_node)

		return expression_nodes


class CoveragePatternSelector(PatternSelector):
	"""
	TODO: describe the strategy used by this PatternSelector

	NOTE: this PatternSelector does not work with operators that take more than one column as input (e.g. correlated columns)
	"""

	MIN_COVERAGE = 0.2
	MAX_CANDIDATE_PATTERNS_exhaustive = 10

	@classmethod
	def _select_patterns_exhaustive(cls, candidate_patterns, nb_rows):
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

	@classmethod
	def _select_patterns_greedy(cls, candidate_patterns, nb_rows):
		# TODO: implement a basic greedy approach
		raise Exception("Not implemented")

	@classmethod
	def select_patterns(cls, patterns, columns, nb_rows):
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
				if col_p["coverage"] < cls.MIN_COVERAGE:
					continue
				candidate_patterns.append(col_p)
			if len(candidate_patterns) == 0:
				print("debug: no pattern selected for column={}".format(col))
				# TODO: maybe do something here
				continue

			# select the patterns that when put toghether give: best coverage, min_overlap
			if len(candidate_patterns) < cls.MAX_CANDIDATE_PATTERNS_exhaustive:
				selected_patterns = cls._select_patterns_exhaustive(candidate_patterns, nb_rows)
			else:
				selected_patterns = cls._select_patterns_greedy(candidate_patterns, nb_rows)

			for col_p in selected_patterns:
				exp_node = ExpressionNode(
					p_id=col_p["p_id"],
					cols_in=[col],
					cols_out=col_p["res_columns"],
					cols_ex=col_p["ex_columns"],
					operator_info=col_p["operator_info"],
					details={
						"coverage": col_p["coverage"],
						"null_coverage": col_p["details"]["null_coverage"]
					},
					pattern_signature=col_p["pattern_signature"])
				expression_nodes.append(exp_node)

		return expression_nodes
