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


evaluate_vectorwise() {
	echo "$(date) [evaluate_vectorwise]"

	wb="$1"
	table="$2"

	output_dir=$wbs_dir/$wb/$table.poc_1_out
	out_table="${table}_out"
	n_input_file=$output_dir/$out_table.csv
	n_schema_file=$output_dir/$out_table.table.sql
	wv_n_schema_file=$output_dir/$out_table.table-vectorwise.sql

	db_name=pbib
	source ~/.ingVWsh

	echo "drop table $out_table\g" | sql $db_name
	$SCRIPT_DIR/../util/VectorWiseify-schema.sh $n_schema_file $wv_n_schema_file > /dev/null
	time $SCRIPT_DIR/../evaluation/main-vectorwise.sh $db_name $n_input_file $wv_n_schema_file $out_table $output_dir
}


evaluate_theoretical() {
	echo "$(date) [evaluate_theoretical]"

	wb="$1"
	table="$2"

	output_dir=$wbs_dir/$wb/$table.poc_1_out-theoretical
	out_table="${table}_out"
	n_input_file=$output_dir/$out_table.csv
	n_schema_file=$output_dir/$out_table.table.sql
	wv_n_schema_file=$output_dir/$out_table.table-vectorwise.sql
	full_file_linecount=$repo_wbs_dir/$wb/samples/$table.linecount

	$SCRIPT_DIR/../util/VectorWiseify-schema.sh $n_schema_file $wv_n_schema_file > /dev/null
	time ./evaluation/main-theoretical.py \
	--schema-file $wv_n_schema_file \
	--table-name $out_table \
	--output-dir $output_dir \
	--full-file-linecount $(cat $full_file_linecount) \
	$n_input_file
}


for wb in $testset_dir/*; do
	for table in $(cat $wb); do
		wb="$(basename $wb)"
		echo $wb $table

		# NOTE: `evaluate_vectorwise` loads data to vectorwise and gathers logs; no other operations should be performed on vectorwise during this time
		# evaluate_vectorwise $wb $table &> $wbs_dir/$wb/$table.poc_1.evaluate.out

		evaluate_theoretical $wb $table &> $wbs_dir/$wb/$table.poc_1.evaluate-theoretical.out		
	done
done


: <<'END_COMMENT'
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test

================================================================================
date; ./poc_1/evaluate_all.sh $wbs_dir; echo $?; date

cat $wbs_dir/*/*.poc_1.evaluate.out | less
cat $wbs_dir/*/*.poc_1.evaluate-theoretical.out | less

END_COMMENT
