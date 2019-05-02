#!/usr/bin/env python3

import os
import sys
import argparse
import json
import pydot
from lib.expression_tree import ExpressionTree


def get_col_vertex_repr(col):
	return "[col]\n{}".format(col.col_id)

def get_node_vertex_repr(node_id, expr_node):
	return "[expr_node]\n{}".format(node_id)


def plot_expression_tree(expr_tree, out_file):
	graph = pydot.Dot(graph_type='graph')

	for col_id in expr_tree.columns:
		col_item = expr_tree.get_column(col_id)
		for node_id in col_item["input_of"]:
			node = expr_tree.get_node(node_id)
			src = get_col_vertex_repr(col_item["col_info"])
			dst = get_node_vertex_repr(node_id, node)
			print(src, dst)
			edge = pydot.Edge(src, dst)
			graph.add_edge(edge)
		for node_id in col_item["output_of"]:
			node = expr_tree.get_node(node_id)
			src = get_node_vertex_repr(node_id, node)
			dst = get_col_vertex_repr(col_item["col_info"])
			print(src, dst)
			edge = pydot.Edge(src, dst)
			graph.add_edge(edge)

	graph.write_svg(out_file)


def read_expr_tree(expr_tree_file):
	with open(expr_tree_file, 'r') as f:
		expr_tree_dict = json.load(f)
		return ExpressionTree.from_dict(expr_tree_dict)


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Visualize expression tree."""
	)

	parser.add_argument('file', help='Expression tree file')
	parser.add_argument('--out-file', dest='out_file', type=str,
		help="Output file to save visualization to")

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	expr_tree = read_expr_tree(args.file)
	plot_expression_tree(expr_tree, args.out_file)


if __name__ == "__main__":
	main()
