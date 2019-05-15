#!/bin/bash

SCRIPT_DIR="$(dirname "$(realpath "$0")")"
WORKING_DIR="$(pwd)"
testset_dir=$SCRIPT_DIR/../testsets/testset_unique_schema


usage() {
cat <<EOM
Usage: "$(basename $0)" <wbs-dir>
  wbs-dir    root directory with all the PBIB workbooks
EOM
}

if [ "$#" -lt 1 ]; then
	usage
	exit 1
fi
wbs_dir="$1"


compare() {
	echo "$(date) [compare]"

	wb="$1"
	table="$2"

	output_dir=$wbs_dir/$wb/$table.poc_1_out/compare_stats
	out_table="${table}_out"
	stats_file_default=$wbs_dir/$wb/$table.evaluation/$table.eval-vectorwise.json
	stats_file_wc=$wbs_dir/$wb/$table.poc_1_out/$out_table.eval-vectorwise.json
	expr_tree_file=$wbs_dir/$wb/$table.expr_tree/expr_tree.json
	apply_expr_stats_file=$wbs_dir/$wb/$table.poc_1_out/$out_table.stats.json

	if test -f "$stats_file_wc"; then
		mkdir -p $output_dir
		$SCRIPT_DIR/../evaluation/compare_stats.py $stats_file_default $stats_file_wc --expr-tree-file $expr_tree_file --apply-expr-stats-file $apply_expr_stats_file &> $output_dir/$table.compare_stats.default-wc.out
	fi
}


for wb in $testset_dir/*; do
	for table in $(cat $wb); do
		wb="$(basename $wb)"
		echo $wb $table

		compare $wb $table &> $wbs_dir/$wb/$table.poc_1.compare.out
	done
done


: <<'END_COMMENT'
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test

================================================================================
./poc_1/compare_all.sh $wbs_dir

cat $wbs_dir/*/*.poc_1.compare.out | less
cat $wbs_dir/*/*.poc_1_out/compare_stats/*.compare_stats.default-wc.out | grep -e "table_compression_ratio" -e "used_compression_ratio="

END_COMMENT
