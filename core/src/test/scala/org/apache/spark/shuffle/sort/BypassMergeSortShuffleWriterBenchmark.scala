/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark

/**
 * Benchmark to measure performance for aggregate primitives.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/<this class>-results.txt".
 * }}}
 */
object BypassMergeSortShuffleWriterBenchmark extends ShuffleWriterBenchmarkBase(false) {

  private var shuffleHandle: BypassMergeSortShuffleHandle[String, String] =
    new BypassMergeSortShuffleHandle[String, String](
      shuffleId = 0,
      numMaps = 1,
      dependency)

  private val MIN_NUM_ITERS = 10

  def constructWriter(transferTo: Boolean): BypassMergeSortShuffleWriter[String, String] = {
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.file.transferTo", String.valueOf(transferTo))
    conf.set("spark.shuffle.file.buffer", "32k")

    val shuffleWriter = new BypassMergeSortShuffleWriter[String, String](
      blockManager,
      blockResolver,
      shuffleHandle,
      0,
      conf,
      taskContext.taskMetrics().shuffleWriteMetrics
    )

    shuffleWriter
  }

  def writeBenchmarkWithLargeDataset(): Unit = {
    val size = 10000000
    val random = new Random(123)
    val data = (1 to size).map { i => {
      val x = random.alphanumeric.take(5).mkString
      Tuple2(x, x)
    } }.toArray
    val benchmark = new Benchmark(
      "BypassMergeSortShuffleWrite (with spill) " + size,
      size,
      minNumIters = MIN_NUM_ITERS,
      output = output)

    addBenchmarkCase(benchmark, "without transferTo") { timer =>
      val shuffleWriter = constructWriter(false)
      timer.startTiming()
      shuffleWriter.write(data.iterator)
      timer.stopTiming()
    }
    addBenchmarkCase(benchmark, "with transferTo") { timer =>
      val shuffleWriter = constructWriter(false)
      timer.startTiming()
      shuffleWriter.write(data.iterator)
      timer.stopTiming()
    }
    benchmark.run()
  }

  def writeBenchmarkWithSmallDataset(): Unit = {
    val size = 10000
    val random = new Random(123)
    val data = (1 to size).map { i => {
      val x = random.alphanumeric.take(5).mkString
      Tuple2(x, x)
    } }.toArray
    val benchmark = new Benchmark("BypassMergeSortShuffleWrite (in memory buffer) " + size,
      size,
      minNumIters = MIN_NUM_ITERS,
      output = output)
    addBenchmarkCase(benchmark, "small dataset without spills on disk") { timer =>
      val shuffleWriter = constructWriter(false)
      timer.startTiming()
      shuffleWriter.write(data.iterator)
      timer.stopTiming()
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("BypassMergeSortShuffleWriter write") {
      writeBenchmarkWithSmallDataset()
      writeBenchmarkWithLargeDataset()
    }
  }
}
