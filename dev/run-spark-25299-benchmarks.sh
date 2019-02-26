#!/usr/bin/env bash

set -ou pipefail

echo "Running SPARK-25299 benchmarks"

SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriterBenchmark"
SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.UnsafeShuffleWriterBenchmark"

echo "Uploading files to remote store"

SPARK_DIR=`pwd`

cp $SPARK_DIR/sql/core/benchmarks/BypassMergeSortShuffleWriterBenchmark-results.txt /tmp/artifacts/
cp $SPARK_DIR/sql/core/benchmarks/UnsafeShuffleWriterBenchmark-results.txt /tmp/artifacts/

