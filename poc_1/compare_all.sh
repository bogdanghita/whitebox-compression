#!/bin/bash

SCRIPT_DIR="$(dirname "$(realpath "$0")")"
WORKING_DIR="$(pwd)"
testset_dir=$SCRIPT_DIR/../testsets/testset_unique_schema_2
repo_wbs_dir=$SCRIPT_DIR/../../public_bi_benchmark-master_project/benchmark


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
	baseline="$3"
	base_dir="$4"

	output_dir=$base_dir/compare_stats
	out_table="${table}_out"
	stats_file_nocompression=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-$baseline.json
	stats_file_default=$wbs_dir/$wb/$table.evaluation/$table.eval-$baseline.json
	stats_file_wc=$base_dir/$out_table.eval-$baseline.json
	expr_tree_file=$wbs_dir/$wb/$table.expr_tree/c_tree.json

	if [ $baseline == "vectorwise" ]; then
		apply_expr_stats_file=$base_dir/$out_table.stats.json
	else
		apply_expr_stats_file=$base_dir/test/$out_table.stats.json
	fi

	summary_out_file_nocompression_default=$output_dir/$table.summary.nocompression-default.json
	summary_out_file_nocompression_wc=$output_dir/$table.summary.nocompression-wc.json
	summary_out_file_default_wc=$output_dir/$table.summary.default-wc.json

	if test -f "$stats_file_wc"; then
		mkdir -p $output_dir
		# nocompression vs default
		$SCRIPT_DIR/../evaluation/compare_stats.py $stats_file_nocompression $stats_file_default \
		--summary-out-file $summary_out_file_nocompression_default\
		&> $output_dir/$table.compare_stats.nocompression-default.out
		# nocompression vs wc
		$SCRIPT_DIR/../evaluation/compare_stats.py $stats_file_nocompression $stats_file_wc \
		--expr-tree-file $expr_tree_file --apply-expr-stats-file $apply_expr_stats_file \
		--summary-out-file $summary_out_file_nocompression_wc\
		&> $output_dir/$table.compare_stats.nocompression-wc.out
		# default vs wc
		$SCRIPT_DIR/../evaluation/compare_stats.py $stats_file_default $stats_file_wc \
		--expr-tree-file $expr_tree_file --apply-expr-stats-file $apply_expr_stats_file \
		--summary-out-file $summary_out_file_default_wc\
		&> $output_dir/$table.compare_stats.default-wc.out
	fi
}


plot_comparison() {
	echo "$(date) [plot_comparison]"

	output_dir=$SCRIPT_DIR/output/output_tmp
	# out_file_format="svg"
	out_file_format="pdf"

	mkdir -p $output_dir
	$SCRIPT_DIR/plot_comparison-vertical.py --wbs-dir $wbs_dir --repo-wbs-dir $repo_wbs_dir --testset-dir $testset_dir --out-dir $output_dir --out-file-format $out_file_format
}


compare_all() {
	for wb in $testset_dir/*; do
		for table in $(cat $wb); do
			wb="$(basename $wb)"
			echo $wb $table

			compare $wb $table vectorwise $wbs_dir/$wb/$table.poc_1_out \
			&> $wbs_dir/$wb/$table.poc_1.compare.out

			compare $wb $table theoretical $wbs_dir/$wb/$table.poc_1_out-theoretical \
			&> $wbs_dir/$wb/$table.poc_1-theoretical.compare.out
		done
	done
}


# compare_all
plot_comparison


: <<'END_COMMENT'
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test

================================================================================
date; ./poc_1/compare_all.sh $wbs_dir; echo $?; date

cat $wbs_dir/*/*.poc_1.compare.out | less
cat $wbs_dir/*/*.poc_1_out/compare_stats/*.compare_stats.default-wc.out | grep -e "table_compression_ratio" -e "used_compression_ratio="

cat $wbs_dir/*/*.poc_1-theoretical.compare.out | less
cat $wbs_dir/*/*.poc_1_out-theoretical/compare_stats/*.compare_stats.default-wc.out | grep -e "table_compression_ratio" -e "used_compression_ratio="

ls -lah ./poc_1/output/output_tmp/*

scp -r bogdan@bricks14:/scratch/bogdan/master-project/whitebox-compression/poc_1/output/output_tmp/* poc_1/output/output_tmp/

END_COMMENT
