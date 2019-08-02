#!/bin/bash

SCRIPT_DIR="$(dirname "$(realpath "$0")")"
WORKING_DIR="$(pwd)"

wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test
repo_wbs_dir=/scratch/bogdan/master-project/public_bi_benchmark-master_project/benchmark
# testset_dir=$SCRIPT_DIR/../../testsets/testset_unique_schema_2
testset_dir=$SCRIPT_DIR/../../testsets/testset_full

evaluate() {
	wb=$1
	table=$2

	train_file=$wbs_dir/$wb/$table.sample-theoretical-train.csv
	test_file=$wbs_dir/$wb/$table.sample-theoretical-test.csv
	schema_file=$repo_wbs_dir/$wb/tables-vectorwise/$table.table-renamed.sql
	full_file_linecount=$repo_wbs_dir/$wb/samples/$table.linecount
	table_name=$table

	output_dir=$wbs_dir/$wb/$table.evaluation
	mkdir -p $output_dir && \
	time $SCRIPT_DIR/../main-theoretical.py \
	--schema-file $schema_file \
	--table-name $table_name \
	--output-dir $output_dir \
	--full-file-linecount $(cat $full_file_linecount) \
	--train-file $train_file \
	--test-file $test_file

	output_dir=$wbs_dir/$wb/$table.evaluation-nocompression
	mkdir -p $output_dir && \
	time $SCRIPT_DIR/../main-theoretical.py \
	--schema-file $schema_file \
	--table-name $table_name \
	--output-dir $output_dir \
	--no-compression \
	--full-file-linecount $(cat $full_file_linecount) \
	--train-file $train_file \
	--test-file $test_file

	stats_file1=$wbs_dir/$wb/$table.evaluation-nocompression/$table.eval-theoretical.json
	stats_file2=$wbs_dir/$wb/$table.evaluation/$table.eval-theoretical.json
	$SCRIPT_DIR/../compare_stats.py $stats_file1 $stats_file2 &> $wbs_dir/$wb/$table.evaluation-theoretical.compare_stats.out
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

# eval all workbooks: baseline-nocompression, baseline-default
nb_procs="$(grep -c ^processor /proc/cpuinfo )"
# nb_procs=300
echo "[parallelism] nb_procs=$nb_procs"

open_sem $nb_procs
for wb in $testset_dir/*; do
	for table in $(cat $wb); do
		wb="$(basename $wb)"
		if [[ "$wb" == "AirlineSentiment" ]]; then
			continue
		fi
		echo "$(date) $wb $table"
		run_with_lock evaluate $wb $table \
		&> $wbs_dir/$wb/$table.main-theoretical.out
		# evaluate $wb $table
	done
done
wait
echo "$(date) [done]"

: <<'END_COMMENT'
wbs_dir=/scratch/bogdan/tableau-public-bench/data/PublicBIbenchmark-test

./evaluation/util/main-theoretical-all.sh

watch tail -n 40 eval_all_workbooks-theoretical.out
cat $wbs_dir/*/*.evaluation-theoretical.compare_stats.out | less
END_COMMENT
