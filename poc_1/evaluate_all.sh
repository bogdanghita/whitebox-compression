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


evaluate() {
	echo "$(date) [evaluate]"

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
	time $SCRIPT_DIR/../evaluation/main.sh $db_name $n_input_file $wv_n_schema_file $out_table $output_dir
}


for wb in $testset_dir/*; do
	for table in $(cat $wb); do
		wb="$(basename $wb)"
		echo $wb $table

		# NOTE: `evaluate` loads data to vectorwise and gathers logs; no other operations should be performed on vectorwise during this time
		evaluate $wb $table &> $wbs_dir/$wb/$table.poc_1.evaluate.out
	done
done


: <<'END_COMMENT'
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test

================================================================================
./poc_1/evaluate_all.sh $wbs_dir

cat $wbs_dir/*/*.poc_1.evaluate.out | less

END_COMMENT
