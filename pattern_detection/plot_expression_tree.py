#!/usr/bin/env python3

import os
import sys
import argparse
import json
import pydot
from lib.util import *
from lib.expression_tree import *


COL_VERTEX_COLOR = {
	"default": "#EEEEEE",
	"exception": "#FFCDD2"
}
EXPR_NODE_VERTEX_COLOR = "#BBDEFB"


def get_col_vertex(col, col_type="default"):
	content = "[{}]\n{}\n{}".format(col.col_id, col.name, col.datatype.to_sql_str())
	return pydot.Node(content, shape="box", style="filled", fillcolor=COL_VERTEX_COLOR[col_type])

def get_node_vertex(node_id, expr_node):
	p_id = expr_node.p_id.replace(":", " ")
	content = "[{node_id}]\n{p_id}\ncoverage={coverage:.2f}\nexceptions={exceptions:.2f}\nnulls={nulls:.2f}".format(
		node_id=node_id,
		p_id=p_id,
		coverage=expr_node.details["coverage"],
		exceptions=(1 - expr_node.details["coverage"] - expr_node.details["null_coverage"]),
		nulls=expr_node.details["null_coverage"])
	return pydot.Node(content, style="filled", fillcolor=EXPR_NODE_VERTEX_COLOR)

def plot_expression_tree(expr_tree, out_file):
	graph = pydot.Dot(graph_type='digraph')

	# create vertices
	col_vertices = {}
	for col_id, col_item in expr_tree.columns.items():
		col_type = "exception" if ExceptionColumnManager.is_exception_col(col_item["col_info"]) else "default"
		c_vertex = get_col_vertex(col_item["col_info"], col_type)
		graph.add_node(c_vertex)
		col_vertices[col_id] = c_vertex
	expr_nodes_vertices = {}
	for node_id, expr_n in expr_tree.nodes.items():
		en_vertex = get_node_vertex(node_id, expr_n)
		graph.add_node(en_vertex)
		expr_nodes_vertices[node_id] = en_vertex

	# add edges
	for col_id, col_item in expr_tree.columns.items():
		for node_id in col_item["input_of"]:
			src = col_vertices[col_id]
			dst = expr_nodes_vertices[node_id]
			edge = pydot.Edge(src, dst)
			graph.add_edge(edge)
		for node_id in col_item["output_of"]:
			src = expr_nodes_vertices[node_id]
			dst = col_vertices[col_id]
			edge = pydot.Edge(src, dst)
			graph.add_edge(edge)

	graph.write_svg(out_file)


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
