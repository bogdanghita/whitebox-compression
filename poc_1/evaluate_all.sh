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
	train_file=$output_dir/train/$out_table.csv
	test_file=$output_dir/test/$out_table.csv
	n_schema_file=$output_dir/test/$out_table.table.sql
	wv_n_schema_file=$output_dir/test/$out_table.table-vectorwise.sql
	full_file_linecount=$repo_wbs_dir/$wb/samples/$table.linecount

	$SCRIPT_DIR/../util/VectorWiseify-schema.sh $n_schema_file $wv_n_schema_file > /dev/null
	time ./evaluation/main-theoretical.py \
	--schema-file $wv_n_schema_file \
	--table-name $out_table \
	--output-dir $output_dir \
	--full-file-linecount $(cat $full_file_linecount) \
	--train-file $train_file \
	--test-file $test_file
}


# https://unix.stackexchange.com/questions/103920/parallelize-a-bash-for-loop
open_sem(){
	mkfifo pipe-$$
	exec 3<>pipe-$$
	rm pipe-$$
	local i=$1
	for((;i>0;i--)); do
		printf %s 000 >&3
	done
}
run_with_lock(){
	local x
	read -u 3 -n 3 x && ((0==x)) || exit $x
	(
		( "$@"; )
		printf '%.3d' $? >&3
	)&
}


vectorwise_all(){
	echo "[vectorwise_all]"
	for wb in $testset_dir/*; do
		for table in $(cat $wb); do
			wb="$(basename $wb)"
			echo "$(date) $wb $table"

			# NOTE: `evaluate_vectorwise` loads data to vectorwise and gathers logs; no other operations should be performed on vectorwise during this time
			evaluate_vectorwise $wb $table &> $wbs_dir/$wb/$table.poc_1.evaluate.out
		done
	done
}


theoretical_all(){
	echo "[theoretical_all]"
	nb_procs="$(grep -c ^processor /proc/cpuinfo )"
	echo "[parallelism] nb_procs=$nb_procs"

	# run in parallel with $nb_procs concurrent tasks
	open_sem $nb_procs
	for wb in $testset_dir/*; do
		for table in $(cat $wb); do
			wb="$(basename $wb)"

			echo "$(date) $wb $table"
			run_with_lock evaluate_theoretical $wb $table &> $wbs_dir/$wb/$table.poc_1.evaluate-theoretical.out
		done
	done
}


vectorwise_all
theoretical_all


: <<'END_COMMENT'
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test

================================================================================
date; ./poc_1/evaluate_all.sh $wbs_dir; echo $?; date

cat $wbs_dir/*/*.poc_1.evaluate.out | less
cat $wbs_dir/*/*.poc_1.evaluate-theoretical.out | less

END_COMMENT
