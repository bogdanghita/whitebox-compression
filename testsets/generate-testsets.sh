#!/bin/bash
set -e
set -o pipefail

SCRIPT_DIR="$(dirname "$(realpath "$0")")"
WORKING_DIR="$(pwd)"
repo_wbs_dir=$SCRIPT_DIR/../../public_bi_benchmark-master_project/benchmark
testset_dir=$SCRIPT_DIR/testset_full

mkdir -p $testset_dir

for wb in $repo_wbs_dir/*; do \
  for table in $wb/samples/*.sample.csv; do \
    wb="$(basename $wb)"; \
    table="$(basename $table)"; table="${table%.sample.csv}"; \
    echo $wb $table; \
\
    echo $table >> $testset_dir/$wb
\
  done; \
done
