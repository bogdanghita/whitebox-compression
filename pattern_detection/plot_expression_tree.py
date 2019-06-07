#!/usr/bin/env python3

import os
import sys
import argparse
import json
import pydot
from lib.util import *
from lib.expression_tree import *


COL_VERTEX_FILLCOLOR = {
	"default": "#FAFAFA",
	"exception": "#FFEBEE"
}
COL_VERTEX_COLOR = {
	"default": "#424242",
	"input": "#1565C0",
	"output": "#F57F17"
}
EXPR_NODE_VERTEX_COLOR = "#EDE7F6"


def get_col_vertex(col, col_type="default", node_type="default", col_value=None):
	content = "[{}]\n{}\n{}".format(col.col_id, col.name, col.datatype.to_sql_str())
	if col_value is not None:
		content += "\n({})".format(col_value)
	color = COL_VERTEX_COLOR[node_type]
	return pydot.Node(content, shape="box", style="filled", color=color, fillcolor=COL_VERTEX_FILLCOLOR[col_type])

def get_compression_node_content(node_id, expr_node, p_id):
	content = "[{node_id}]\n{p_id}\ncoverage={coverage:.2f}\nexceptions={exceptions:.2f}\nnulls={nulls:.2f}".format(
		node_id=node_id,
		p_id=p_id,
		coverage=expr_node.details["coverage"],
		exceptions=(1 - expr_node.details["coverage"] - expr_node.details["null_coverage"]),
		nulls=expr_node.details["null_coverage"])
	return content

def get_decompression_node_content(node_id, expr_node, p_id):
	content = "[{node_id}]\n{operator_name}".format(
		node_id=node_id,
		operator_name=expr_node.operator_info["name"])
	return content

def get_node_vertex(node_id, expr_node):
	p_id = expr_node.p_id.replace(":", " ")
	if isinstance(expr_node, CompressionNode):
		content = get_compression_node_content(node_id, expr_node, p_id)
	else: # DecompressionNode
		content = get_decompression_node_content(node_id, expr_node, p_id)
	return pydot.Node(content, style="filled", fillcolor=EXPR_NODE_VERTEX_COLOR)

def plot_expression_tree(expr_tree, out_file, debug_values_file=None, ignore_unused_columns=False):
	in_columns = set(expr_tree.get_in_columns())
	out_columns = set(expr_tree.get_out_columns())
	graph = pydot.Dot(graph_type='digraph')

	debug_values = {}
	if debug_values_file is not None:
		with open(debug_values_file, 'r') as fd:
			debug_values = json.load(fd)

	# create vertices
	unused_columns = expr_tree.get_unused_columns()
	col_vertices = {}
	for col_id, col_item in expr_tree.columns.items():
		if ignore_unused_columns and col_id in unused_columns:
			continue
		col_type = "exception" if OutputColumnManager.is_exception_col(col_item["col_info"]) else "default"
		node_type = "output" if col_id in out_columns else "input" if col_id in in_columns else "default"
		col_value = None if col_id not in debug_values else debug_values[col_id]
		c_vertex = get_col_vertex(col_item["col_info"],
								  col_type=col_type,
								  node_type=node_type,
								  col_value=col_value)
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
		help="Output file to save visualization to",
		required=True)
	parser.add_argument('--debug-values-file', dest='debug_values_file', type=str,
		help="Debug file containing values for columns")

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	expr_tree = read_expr_tree(args.file)
	plot_expression_tree(expr_tree, args.out_file, args.debug_values_file)


if __name__ == "__main__":
	main()


"""
input_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.expr_tree/c_tree.json
output_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.expr_tree/c_tree.svg

./pattern_detection/plot_expression_tree.py $input_file --out-file $output_file

================================================================================
[debug_values]
input_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.expr_tree/c_tree.json
c_output_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.expr_tree/c_debug_c_tree.svg
dec_output_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.expr_tree/dec_debug_c_tree.svg
c_debug_values_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.expr_tree/c_debug_values.json
dec_debug_values_file=/export/scratch1/bogdan/tableau-public-bench/data/PublicBIbenchmark-poc_1/CommonGovernment/CommonGovernment_1.expr_tree/dec_debug_values.json

./pattern_detection/plot_expression_tree.py $input_file --out-file $c_output_file --debug-values-file $c_debug_values_file
./pattern_detection/plot_expression_tree.py $input_file --out-file $dec_output_file --debug-values-file $dec_debug_values_file
"""
