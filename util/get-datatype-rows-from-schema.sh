#!/bin/bash

SCRIPT_DIR=$(dirname "$(realpath "$0")")
WORKING_DIR=$(pwd)
PBIB_REPO_DIR=$SCRIPT_DIR/../../public_bi_benchmark-master_project

for table_f in $PBIB_REPO_DIR/benchmark/*/tables/*.sql
do
	table=$(basename $table_f); table="${table%.table.sql}"
	echo $table_f $table
	lines=$(head -n -1 $table_f | tail -n +2)

	datatypes_row=""
	header_row=""

	while read -r l; do
		echo $l
		l="${l%,}"
		datatype="${l#*\" }"
		colname="${l%\" *}"; colname="${colname#\"}"
		echo "colname: $colname, datatype: $datatype"
		datatypes_row="$datatypes_row|$datatype"
		header_row="$header_row|$colname"
	done <<< "$lines"

	datatypes_row="${datatypes_row#|}"
	header_row="${header_row#|}"

	echo $datatypes_row
	echo $header_row

	datatypes_out_f=$(dirname $table_f); datatypes_out_f="$datatypes_out_f/../samples/$table.datatypes.csv"
	header_out_f=$(dirname $table_f); header_out_f="$header_out_f/../samples/$table.header.csv"
	echo $datatypes_row > $datatypes_out_f
	echo $header_row > $header_out_f

	sample_f=$(dirname $table_f); sample_f="$sample_f/../samples/$table.sample.csv"
	combined_out_f=$(dirname $table_f); combined_out_f="$combined_out_f/../samples/$table.combined.csv"
	cat $header_out_f > $combined_out_f
	cat $datatypes_out_f >> $combined_out_f
	cat $sample_f >> $combined_out_f

done
