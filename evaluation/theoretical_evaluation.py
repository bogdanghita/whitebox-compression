import os
import sys
from lib.util import *


def init_columns(schema):
	res = []
	for col_id, col_item in schema.items():
		datatype = DataType.from_sql_str(col_item["datatype"])
		res.append(Column(col_id, col_item["col_name"], datatype))
	return res


def init_estimators(columns, null_value):
	res = [
		NoCompressionEstimator(columns, null_value),
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


def main(schema, input_file, fdelim, null_value):
	"""
	Returns:
		stats: dict(col_id, estimators) where:
			col_id: # id of the column
			estimators: dict(name, results) where:
				name: # name of the estimator
				results: dict(
					size_B: # total size of the column in bytes
					details: dict()
				)
	"""

	columns = init_columns(schema)
	estimator_list = init_estimators(columns, null_value)

	with open(input_file, 'r') as fd:
		driver = FileDriver(fd)
		driver_loop(driver, estimator_list, fdelim, null_value)

	stats = {col.col_id: {} for col in columns}
	for estimator in estimator_list:
		res = estimator.evaluate()
		for col_id, size_B in res.items():
			stats[col_id][estimator.name] = {
				"size_B": size_B,
				"details": dict()
			}

	return stats
