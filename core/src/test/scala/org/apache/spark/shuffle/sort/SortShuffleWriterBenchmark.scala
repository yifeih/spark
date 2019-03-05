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

import org.mockito.Mockito.when

import org.apache.spark.{Aggregator, SparkEnv}
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.util.Utils

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
object SortShuffleWriterBenchmark extends ShuffleWriterBenchmarkBase {

  private var shuffleHandle: BaseShuffleHandle[String, String, String] =
    new BaseShuffleHandle(
      shuffleId = 0,
      numMaps = 1,
      dependency = dependency)

  private val DEFAULT_DATA_STRING_SIZE = 5
  private val MIN_NUM_ITERS = 5

  def getWriter(aggregator: Option[Aggregator[String, String, String]],
                sorter: Option[Ordering[String]]): SortShuffleWriter[String, String, String] = {
    // we need this since SortShuffleWriter uses SparkEnv to get lots of its private vars
    val defaultSparkEnv = SparkEnv.get
    SparkEnv.set(new SparkEnv(
      "0",
      null,
      serializer,
      null,
      serializerManager,
      null,
      null,
      null,
      blockManager,
      null,
      null,
      null,
      null,
      defaultConf
    ))

    if (aggregator.isEmpty && sorter.isEmpty) {
      when(dependency.mapSideCombine).thenReturn(false)
    } else {
      when(dependency.mapSideCombine).thenReturn(false)
      when(dependency.aggregator).thenReturn(aggregator)
      when(dependency.keyOrdering).thenReturn(sorter)
    }

    when(taskContext.taskMemoryManager()).thenReturn(taskMemoryManager)

    val shuffleWriter = new SortShuffleWriter[String, String, String](
      blockResolver,
      shuffleHandle,
      0,
      taskContext
    )
    shuffleWriter
  }

  def writeBenchmarkWithSmallDataset(): Unit = {
    val size = 1000
    val benchmark = new Benchmark("SortShuffleWriter with spills",
      size,
      minNumIters = MIN_NUM_ITERS,
      output = output)
    addBenchmarkCase(benchmark, "small dataset without spills") { timer =>
      val writer = getWriter(Option.empty, Option.empty)
      val array = createDataInMemory(1000)
      timer.startTiming()
      writer.write(array.iterator)
      timer.stopTiming()
      assert(tempFilesCreated.length == 0)
    }
    benchmark.run()
  }

  def writeBenchmarkWithSpill(): Unit = {
    val size = PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES/4/DEFAULT_DATA_STRING_SIZE
    val dataFile = createDataOnDisk(size)

    val benchmark = new Benchmark("SortShuffleWriter with spills",
      size,
      minNumIters = MIN_NUM_ITERS,
      output = output,
      outputPerIteration = true)
    addBenchmarkCase(benchmark, "no map side combine") { timer =>
      val shuffleWriter = getWriter(Option.empty, Option.empty)
      Utils.tryWithResource(DataIterator(inputFile = dataFile, DEFAULT_DATA_STRING_SIZE)) {
        iterator =>
          timer.startTiming()
          shuffleWriter.write(iterator)
          timer.stopTiming()
      }
      assert(tempFilesCreated.length == 8)
    }

    def createCombiner(i: String): String = i
    def mergeValue(i: String, j: String): String = if (Ordering.String.compare(i, j) > 0) i else j
    def mergeCombiners(i: String, j: String): String =
      if (Ordering.String.compare(i, j) > 0) i else j
    val aggregator =
      new Aggregator[String, String, String](createCombiner, mergeValue, mergeCombiners)
    addBenchmarkCase(benchmark, "with map side aggregation") { timer =>
      val shuffleWriter = getWriter(Some(aggregator), Option.empty)
      Utils.tryWithResource(DataIterator(inputFile = dataFile, DEFAULT_DATA_STRING_SIZE)) {
        iterator =>
          timer.startTiming()
          shuffleWriter.write(iterator)
          timer.stopTiming()
      }
      assert(tempFilesCreated.length == 8)
    }

    val sorter = Ordering.String
    addBenchmarkCase(benchmark, "with map side sort") { timer =>
      val shuffleWriter = getWriter(Option.empty, Some(sorter))
      Utils.tryWithResource(DataIterator(inputFile = dataFile, DEFAULT_DATA_STRING_SIZE)) {
        iterator =>
          timer.startTiming()
          shuffleWriter.write(iterator)
          timer.stopTiming()
      }
      assert(tempFilesCreated.length == 8)
    }
    benchmark.run()
    dataFile.delete()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("SortShuffleWriter writer") {
      writeBenchmarkWithSmallDataset()
      writeBenchmarkWithSpill()
    }
  }
}
