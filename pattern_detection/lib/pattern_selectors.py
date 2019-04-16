import os
import sys
from lib.util import ExpressionNode


class PatternSelector(object):
	@classmethod
	def select_patterns(cls, patterns, columns, nb_rows):
		""" Selects the best (combination of) pattern(s) for the given columns

		Returns:
			expression_nodes: list(
				dict(
					p_id: # id of the pattern
					rows: list() # rows where the pattern applies
					cols_in: list(col_id1, col_id2, ...) # list of input columns; type: util.Column
					cols_out: [rcol_1, rcol_2, ...], # list of resulting columns; type: util.Column
					operator_info: dict() # operator parameters (used when applying the transformation)
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

	MIN_SCORE = 0.4

	@classmethod
	def select_patterns(cls, patterns, columns, nb_rows):
		expression_nodes = []

		for col in columns:
			# TODO: get column nulls
			# NOTE: each pattern detector also outputs nulls; however, there should be only one "nulls" per column

			col_patterns = []
			for p_name, p_columns in patterns.items():
				if col.col_id not in p_columns["columns"]:
					continue
				col_patterns.extend(p_columns["columns"][col.col_id])

			# choose the best pattern with score higher than MIN_SCORE
			max_score, best_p = -1, None
			for col_p in col_patterns:
				if col_p["score"] < cls.MIN_SCORE:
					continue
				if col_p["score"] > max_score:
					max_score, best_p = col_p["score"], col_p
			if best_p is None:
				print("debug: no pattern selected for column={}".format(col))
				# TODO: maybe do something here
			else:
				exp_node = ExpressionNode(
					p_id=best_p["p_id"],
					rows=best_p["rows"],
					cols_in=[col],
					cols_out=best_p["res_columns"],
					operator_info=best_p["operator_info"],
					details={
						"score": best_p["score"]
					})
				expression_nodes.append(exp_node)

		return expression_nodes
