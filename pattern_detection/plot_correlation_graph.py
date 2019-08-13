#!/usr/bin/env python3

import os
import sys
import argparse
import json
import pydot
from lib.util import *
from lib.expression_tree import *


EDGE_COLOR = {
	True: "red",
	False: "black"
}


def get_vertex(col_id):
	content = col_id
	return pydot.Node(content, style="filled")

def get_edge(source_col_id, target_col_id, corr_coef, selected):
	label = "{:.2f}".format(corr_coef)
	return pydot.Edge(source_col_id, target_col_id, label=label, color=EDGE_COLOR[selected])


def plot_correlation_graph(corrs, out_file):
	graph = pydot.Dot(graph_type='digraph')
	# graph = pydot.Dot(graph_type='digraph', margin=0)

	vertices = {}
	for ((source_col_id, target_col_id, corr_coef), selected) in corrs:
		source_vertex, target_vertex = get_vertex(source_col_id), get_vertex(target_col_id)
		if source_col_id not in vertices:
			graph.add_node(source_vertex)
			vertices[source_col_id] = source_vertex
		if target_col_id not in vertices:
			graph.add_node(target_vertex)
			vertices[target_col_id] = target_vertex
		edge = get_edge(source_col_id, target_col_id, corr_coef, selected)
		graph.add_edge(edge)

	graph.write_svg(out_file)
	# graph.write_pdf(out_file)


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Graph visualization of column correlations."""
	)

	parser.add_argument('file', help='Correlations file')
	parser.add_argument('--out-file', dest='out_file', type=str,
		help="Output file to save visualization to",
		required=True)

	return parser.parse_args()


def main():
	args = parse_args()
	print(args)

	with open(args.file, 'r') as fp:
		corrs = json.load(fp)
	plot_correlation_graph(corrs, args.out_file)


if __name__ == "__main__":
	main()
