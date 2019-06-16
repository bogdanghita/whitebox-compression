#!/usr/bin/env python3

import os
import sys
import argparse
import json
from random import randint


MAXINT = sys.maxsize
MININT = -sys.maxsize
ROW_SIZE_SAMPLE_POINTS = 1024


def get_sample(fp, nb_dataset_rows, nb_sample_points, nb_block_rows, file_name=None):
	print("[get_sample][initial-params] nb_dataset_rows={}, nb_sample_points={}, nb_block_rows={}".format(nb_dataset_rows, nb_sample_points, nb_block_rows))

	if nb_sample_points * nb_block_rows > nb_dataset_rows / 2:
		nb_sample_points = int(nb_dataset_rows / 2 / nb_block_rows)
		if nb_sample_points * nb_block_rows > nb_dataset_rows:
			nb_sample_points = 1
			nb_block_rows = nb_dataset_rows

	print("[get_sample][final-params] nb_dataset_rows={}, nb_sample_points={}, nb_block_rows={}".format(nb_dataset_rows, nb_sample_points, nb_block_rows))

	fp_line_idx = 0
	step = int(nb_dataset_rows / nb_sample_points)
	# print("[get_sample] step={}".format(step))

	for i in range(0, nb_dataset_rows - (step - nb_block_rows), step):
		rand_start, rand_end = i, i + step - nb_block_rows
		# print("rand_start={}, rand_end={}".format(rand_start, rand_end))
		block_start = randint(rand_start, rand_end)
		# print("block_start={}, block_end={}".format(block_start, block_start + nb_block_rows))

		# seek until block start
		while fp_line_idx < block_start:
			if fp.readline() == "":
				raise Exception("Unexpected end of file {}".format(file_name))
			fp_line_idx += 1

		# read nb_block_rows lines
		for j in range(nb_block_rows):
			line = fp.readline()
			if line == "":
				raise Exception("Unexpected end of file: {}".format(file_name))
			fp_line_idx += 1
			yield line


def get_row_stats(rows):
	stats = {
		"avg_size": 0,
		"max_size": MININT,
		"min_size": MAXINT
	}

	size_sum = 0
	nb_rows = 0
	for r in rows:
		size = len(r)
		size_sum += size
		nb_rows += 1
		if size > stats["max_size"]:
			stats["max_size"] = size
		if size < stats["min_size"]:
			stats["min_size"] = size
	stats["avg_size"] = size_sum / max(1, nb_rows)

	return stats


def output_sample(sample, output_file):
	with open(output_file, 'w') as fp:
		for row in sample:
			fp.write(row)


def parse_args():
	parser = argparse.ArgumentParser(
		description="""Extract a sample from a CSV file."""
	)

	parser.add_argument('file', help='CSV file to process')
	parser.add_argument('--dataset-nb-rows', dest='dataset_nb_rows', type=int,
		help="Total number of rows in the full dataset",
		required=True)
	parser.add_argument('--max-sample-size', dest='max_sample_size', type=int,
		help="(approximative) maximim total size of the sample (in bytes). The size of the resulting sample may slightly vary",
		required=True)
	parser.add_argument('--sample-block-nb-rows', dest='sample_block_nb_rows', type=int,
		help="Number of consecutive rows that form a sample block",
		required=True)
	parser.add_argument('--output-file', dest='output_file', type=str,
		help="Path to file where where to ouput the sample",
		required=True)

	return parser.parse_args()


def main():
	args = parse_args()
	print("args: {}".format(args))

	with open(args.file, 'r') as fp:
		row_size_sample = get_sample(fp,
									 nb_dataset_rows=args.dataset_nb_rows,
									 nb_sample_points=ROW_SIZE_SAMPLE_POINTS,
									 nb_block_rows=1)
		row_stats = get_row_stats(row_size_sample)
		print("row_stats: {}".format(row_stats))

		# TODO: maybe use some other row metric instead of "avg_size"; ex: median
		nb_sample_points = max(1, int(args.max_sample_size / (args.sample_block_nb_rows * max(1, row_stats["avg_size"]))))

		fp.seek(0)
		sample = get_sample(fp,
							nb_dataset_rows=args.dataset_nb_rows,
							nb_sample_points=nb_sample_points,
							nb_block_rows=args.sample_block_nb_rows,
							file_name=args.file)

		output_sample(sample, args.output_file)


if __name__ == "__main__":
	main()


"""
wbs_dir=/ufs/bogdan/work/master-project/public_bi_benchmark-master_project/benchmark
max_sample_size=$((1024*1024*10))

dataset_nb_rows=20
./main.py --dataset-nb-rows $dataset_nb_rows --max-sample-size $max_sample_size --sample-block-nb-rows 2 --output-file ./output/output.csv $wbs_dir/Arade/samples/Arade_1.sample.csv

================================================================================

wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
max_sample_size=$((1024*1024*10))

dataset_nb_rows=9888775
./main.py --dataset-nb-rows $dataset_nb_rows --max-sample-size $max_sample_size --sample-block-nb-rows 32 --output-file ./output/output.csv $wbs_dir/Arade/Arade_1.csv

dataset_nb_rows=9624351
./main.py --dataset-nb-rows $dataset_nb_rows --max-sample-size $max_sample_size --sample-block-nb-rows 32 --output-file ./output/output.csv $wbs_dir/NYC/NYC_1.csv
"""
