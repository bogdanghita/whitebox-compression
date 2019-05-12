#!/bin/bash
set -e
set -o pipefail

SCRIPT_DIR="$(dirname "$(realpath "$0")")"
WORKING_DIR="$(pwd)"
repo_wbs_dir=$SCRIPT_DIR/../../public_bi_benchmark-master_project/benchmark


usage() {
cat <<EOM
Usage: "$(basename $0)" <wbs-dir> <wb> <table>
  wbs-dir    root directory with all the PBIB workbooks
  wb         name of the target workbook
  table      name of the target table
EOM
}

if [ "$#" -lt 3 ]; then
	usage
	exit 1
fi
wbs_dir="$1"
wb="$2"
table="$3"


generate_sample() {
	set -e
	set -o pipefail
	echo "$(date) [generate_sample]"

	sample_file=$wbs_dir/$wb/$table.sample.csv
	if test -f "$sample_file"; then
		echo "debug: skipping sampling; sample already exists"
		return
	fi

	max_sample_size=$((1024*1024*10))
	dataset_nb_rows=$(cat $repo_wbs_dir/$wb/samples/$table.linecount)

	$SCRIPT_DIR/../sampling/main.py --dataset-nb-rows $dataset_nb_rows --max-sample-size $max_sample_size --sample-block-nb-rows 64 --output-file $sample_file $wbs_dir/$wb/$table.csv
}

generate_expression() {
	set -e
	set -o pipefail
	echo "$(date) [generate_expression]"

	sample_file=$wbs_dir/$wb/$table.sample.csv
	pattern_distr_out_dir=$wbs_dir/$wb/$table.patterns
	ngram_freq_masks_output_dir=$wbs_dir/$wb/$table.ngram_freq_masks
	expr_tree_output_dir=$wbs_dir/$wb/$table.expr_tree

	mkdir -p $pattern_distr_out_dir $ngram_freq_masks_output_dir $expr_tree_output_dir

	$SCRIPT_DIR/../pattern_detection/main.py --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv --pattern-distribution-output-dir $pattern_distr_out_dir --ngram-freq-masks-output-dir $ngram_freq_masks_output_dir --expr-tree-output-dir $expr_tree_output_dir $sample_file
}

apply_expression() {
	set -e
	set -o pipefail
	echo "$(date) [apply_expression]"

	input_file=$wbs_dir/$wb/$table.csv
	expr_tree_file=$wbs_dir/$wb/$table.expr_tree/expr_tree.json
	output_dir=$wbs_dir/$wb/$table.poc_1_out
	out_table="${table}_out"

	mkdir -p $output_dir

	time $SCRIPT_DIR/../pattern_detection/apply_expression.py --expr-tree-file $expr_tree_file --header-file $repo_wbs_dir/$wb/samples/$table.header-renamed.csv --datatypes-file $repo_wbs_dir/$wb/samples/$table.datatypes.csv --output-dir $output_dir --out-table-name $out_table $input_file
}

evaluate() {
	set -e
	set -o pipefail
	echo "$(date) [evaluate]"

	output_dir=$wbs_dir/$wb/$table.poc_1_out
	out_table="${table}_out"
	n_input_file=$output_dir/$out_table.csv
	n_schema_file=$output_dir/$out_table.table.sql
	wv_n_schema_file=$output_dir/$out_table.table-vectorwise.sql

	db_name=pbib
	source ~/.ingVWsh

	echo "drop table $out_table\g" | sql $db_name
	$SCRIPT_DIR/../util/VectorWiseify-schema.sh $n_schema_file $wv_n_schema_file > /dev/null
	time $SCRIPT_DIR/../evaluation/main.sh $db_name $n_input_file $wv_n_schema_file $out_table $output_dir
}


generate_sample
generate_expression
apply_expression
# NOTE: `evaluate` loads data to vectorwise and gathers logs; no other operations should be performed on vectorwise during this time
evaluate


: <<'END_COMMENT'
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=../public_bi_benchmark-master_project/benchmark

================================================================================
wb=CommonGovernment
table=CommonGovernment_1
================================================================================
wb=Eixo
table=Eixo_1
================================================================================
wb=Arade
table=Arade_1


================================================================================
./poc_1/main.sh $wbs_dir $wb $table


================================================================================
# [evaluation-only]
output_dir=$wbs_dir/$wb/$table.poc_1_out
out_table="${table}_out"
n_input_file=$output_dir/$out_table.csv
n_schema_file=$output_dir/$out_table.table.sql
wv_n_schema_file=$output_dir/$out_table.table-vectorwise.sql
db_name=pbib
source ~/.ingVWsh

echo "drop table $out_table\g" | sql $db_name
./util/VectorWiseify-schema.sh $n_schema_file $wv_n_schema_file > /dev/null
time ./evaluation/main.sh $db_name $n_input_file $wv_n_schema_file $out_table $output_dir


================================================================================
# [run-all]
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=../public_bi_benchmark-master_project/benchmark
testset_dir=testsets/testset_unique_schema

for wb in $testset_dir/*; do \
  for table in $(cat $wb); do \
    wb="$(basename $wb)"; \
    echo $wb $table; \
\
    ./poc_1/main.sh $wbs_dir $wb $table &> $wbs_dir/$wb/$table.poc_1.out; \
\
  done; \
done &> ./poc_1_all_workbooks.out

watch tail -n 40 poc_1_all_workbooks.out
cat $wbs_dir/*/*.poc_1.out | less

END_COMMENT
