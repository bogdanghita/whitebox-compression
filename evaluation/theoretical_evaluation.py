import os
import sys
from lib.util import *
from pattern_detection.patterns import *


def init_columns(schema):
	res = []
	for col_id, col_item in schema.items():
		datatype = DataType.from_sql_str(col_item["datatype"])
		res.append(Column(col_id, col_item["col_name"], datatype))
	return res


def init_estimators(columns, null_value, no_compression=False):
	if no_compression:
		res = [
			NoCompressionEstimator(columns, null_value)
		]
	else:
		res = [
			NoCompressionEstimator(columns, null_value),
			BitsEstimator(columns, null_value),
			DictEstimator(columns, null_value),
			RleEstimator(columns, null_value),
			ForEstimator(columns, null_value)
		]
	return res


def driver_loop(driver, estimator_list, fdelim, null_value):
	total_tuple_count = 0

	while True:
		line = driver.nextTuple()
		if line is None:
			break
		total_tuple_count += 1

		tpl = line.split(fdelim)

		for estimator in estimator_list:
			estimator.feed_tuple(tpl)

		# debug: print progress
		if total_tuple_count % 100000 == 0:
			print("[progress] total_tuple_count={}M".format(
				float(total_tuple_count) / 1000000))
		# end-debug

	return total_tuple_count


def main(schema, input_file, full_file_linecount, fdelim, null_value, no_compression=False):
	"""
	Returns:
		stats: dict(col_id, estimators) where:
			col_id: # id of the column
			estimators: dict(name, results) where:
				name: # name of the estimator
				results: dict(
					size_B: # total size of the column in bytes (extrapolated to the size of the full data)
					details: dict()
				)
	"""

	columns = init_columns(schema)
	estimator_list = init_estimators(columns, null_value, no_compression)

	with open(input_file, 'r') as fd:
		driver = FileDriver(fd)
		sample_tuple_count = driver_loop(driver, estimator_list, fdelim, null_value)

	sample_ratio = float(full_file_linecount) / sample_tuple_count

	stats = {col.col_id: {} for col in columns}
	for estimator in estimator_list:
		res = estimator.evaluate()
		for col_id, (values_size, metadata_size, exceptions_size, null_size) in res.items():
			# debug
			# print("[col_id={}][{}] values_size={}, metadata_size={}, exceptions_size={}, null_size={}".format(
			# 		col_id, estimator.name, values_size, metadata_size, exceptions_size, null_size))
			print("[col_id={}][{}] values_size={}, metadata_size={}, exceptions_size={}, null_size={}".format(
					col_id, estimator.name, sizeof_fmt(values_size), sizeof_fmt(metadata_size), sizeof_fmt(exceptions_size), sizeof_fmt(null_size)))
			# end-debug

			# extrapolate the size for the full data
			full_size = sample_ratio * (values_size + exceptions_size + null_size)
			size_B = metadata_size + full_size
			stats[col_id][estimator.name] = {
				"size_B": size_B,
				"size_human_readable": sizeof_fmt(size_B),
				"details": dict()
			}

	return stats
