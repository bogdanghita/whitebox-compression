#!/bin/bash

SCRIPT_DIR="$(dirname "$(realpath "$0")")"
WORKING_DIR="$(pwd)"

usage() {
cat <<EOM
Usage: "$(basename $0)" <schema-file> <output-file>
  schema-file         SQL file with schema
  output-file         file to write new schema to
EOM
}

if [ "$#" -ne 2 ]; then
	usage
	exit 1
fi
SCHEMA_FILE="$1"
OUTPUT_FILE="$2"

module_name="1-VectorWiseify-tables"
python_script=$SCRIPT_DIR/../../public_bi_benchmark-master_project/scripts/vectorwise/lib/$module_name.py

python -c "$(cat<<EOM
module_name="$module_name"
python_script="$python_script"
schema_file="$SCHEMA_FILE"
output_file="$OUTPUT_FILE"

import imp
vw_schema = imp.load_source(module_name, python_script)

vw_schema.vectorwiseify_table(schema_file, output_file)
EOM
)"
