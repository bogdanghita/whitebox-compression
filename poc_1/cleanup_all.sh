#!/bin/bash

SCRIPT_DIR="$(dirname "$(realpath "$0")")"
WORKING_DIR="$(pwd)"
repo_wbs_dir=$SCRIPT_DIR/../../public_bi_benchmark-master_project/benchmark
testset_dir=$SCRIPT_DIR/../testsets/testset_unique_schema_2


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


for wb in $testset_dir/*; do
	for table in $(cat $wb); do
		wb="$(basename $wb)"

		rm -rf $wbs_dir/$wb/$table.poc_1_out
		rm -rf $wbs_dir/$wb/$table.poc_1*
		rm -rf $wbs_dir/$wb/$table.expr_nodes
		rm -rf $wbs_dir/$wb/$table.expr_tree
		rm -rf $wbs_dir/$wb/$table.ngram_freq_masks
		rm -rf $wbs_dir/$wb/$table.patterns
		rm -rf $wbs_dir/$wb/$table.corr_coefs
	done
done


: <<'END_COMMENT'
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test

================================================================================
date; ./poc_1/cleanup_all.sh $wbs_dir; echo $?; date

END_COMMENT
