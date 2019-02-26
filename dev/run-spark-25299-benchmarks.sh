#!/usr/bin/env bash

set -ou pipefail

echo "Running SPARK-25299 benchmarks"


SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.BypassMergeSortShuffleWriterBenchmark"
SPARK_GENERATE_BENCHMARK_FILES=1 ./build/sbt "sql/test:runMain org.apache.spark.shuffle.sort.UnsafeShuffleWriterBenchmark"

echo "Uploading files to remote store"

SPARK_DIR=`pwd`
BENCHMARK_REPO="spark-25299-benchmark-results"
SVC_USER="svc-spark-25299"
SVC_PASSWORD=${SVC_SPARK_25299_PASSWORD}
cd ~
if [ ! -d $BENCHMARK_REPO ]; then
    git clone https://${SVC_USER}:${SVC_PASSWORD}@github.com/yifeih/${BENCHMARK_REPO}.git
fi
cd $BENCHMARK_REPO
COMMIT_HASH=$(echo $CIRCLE_SHA1 | awk '{print substr($0,0,10)}')
git checkout -b perf-tests-$COMMIT_HASH

mkdir $COMMIT_HASH && cd $COMMIT_HASH
cp $SPARK_DIR/sql/core/benchmarks/BypassMergeSortShuffleWriterBenchmark-results.txt .
cp $SPARK_DIR/sql/core/benchmarks/UnsafeShuffleWriterBenchmark-results.txt .

git add *
git commit -m "Benchmark tests for commit $COMMIT_HASH"
git push origin perf-tests-$COMMIT_HASH

message='{"title": "Benchmark tests for '
message+=$COMMIT_HASH
message+='.", '
message+='"body": "Benchmark tests", '
message+='"head": "perf-tests-'
message+=$COMMIT_HASH
message+='", '
message+='"base": "master" }'

curl -XPOST https://${SVC_USER}:${SVC_PASSWORD}@api.github.com/repos/yifeih/spark-25299-benchmark-results/pulls -d \'$message\'


