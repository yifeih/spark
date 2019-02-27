#!/usr/bin/env bash

set -ou pipefail

echo "Running SPARK-25299 benchmarks"

SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriterBenchmark"
SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.UnsafeShuffleWriterBenchmark"

echo "Uploading files to remote store"

SPARK_DIR=`pwd`

mkdir -p /tmp/artifacts
cp $SPARK_DIR/sql/core/benchmarks/BypassMergeSortShuffleWriterBenchmark-results.txt /tmp/artifacts/
cp $SPARK_DIR/sql/core/benchmarks/UnsafeShuffleWriterBenchmark-results.txt /tmp/artifacts/


RESULTS=""
for benchmark_file in /tmp/artifacts/*.txt; do
    RESULTS+=$(cat $benchmark_file)
done

PULL_REQUEST_NUM=$(git log -1 | sed "5q;d" | awk '{print $NF}' | sed 's/(//g' | sed 's/)//g' | sed 's/#//g')


USERNAME=svc-spark-25299
PASSWORD=$SVC_SPARK_25299_PASSWORD
message='{"body": "```'
message+=$results
message+='```", "event":"COMMENT"}'
echo $message
curl -XPOST https://${USERNAME}:${PASSWORD}@api.github.com/repos/palantir/spark/pulls/${PULL_REQUEST_NUM}/reviews -d \'$message\'
