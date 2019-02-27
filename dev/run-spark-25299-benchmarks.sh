#!/usr/bin/env bash

set -ou pipefail


function usage {
    echo "Usage: $(basename $0) [-h] [-u]"
    echo ""
    echo "Runs the perf tests and optionally uploads the results as a comment to a PR"
    echo ""
    echo "    -h           help"
    echo "    -u           Upload the perf results as a comment"
    # Exit as error for nesting scripts
    exit 1
}

UPLOAD=false
while getopts "hu" opt; do
  case $opt in
    h)
      usage
      exit 0;;
    u)
      UPLOAD=true;;
  esac
done

echo "Running SPARK-25299 benchmarks"

SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriterBenchmark"
SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.UnsafeShuffleWriterBenchmark"

SPARK_DIR=`pwd`

mkdir -p /tmp/artifacts
cp $SPARK_DIR/sql/core/benchmarks/BypassMergeSortShuffleWriterBenchmark-results.txt /tmp/artifacts/
cp $SPARK_DIR/sql/core/benchmarks/UnsafeShuffleWriterBenchmark-results.txt /tmp/artifacts/

if [ "$UPLOAD" = false ]; then
    exit 0
fi

IFS=
RESULTS=""
for benchmark_file in /tmp/artifacts/*.txt; do
    RESULTS+=$(cat $benchmark_file)
    RESULTS+=$'\n\n'
done

echo $RESULTS
PULL_REQUEST_NUM=$(git log -1 | sed "5q;d" | awk '{print $NF}' | sed 's/(//g' | sed 's/)//g' | sed 's/#//g')


USERNAME=svc-spark-25299
PASSWORD=$SVC_SPARK_25299_PASSWORD
message='{"body": "```'
message+=$'\n'
message+=$RESULTS
message+=$'\n'
json_message=$(echo $message | awk '{printf "%s\\n", $0}')
json_message+='```", "event":"COMMENT"}' 
echo "$json_message" > benchmark_results.json

echo "Sending benchmark requests to PR $PULL_REQUEST_NUM"
curl -XPOST https://${USERNAME}:${PASSWORD}@api.github.com/repos/palantir/spark/pulls/${PULL_REQUEST_NUM}/reviews -d @benchmark_results.json
rm benchmark_results.json
