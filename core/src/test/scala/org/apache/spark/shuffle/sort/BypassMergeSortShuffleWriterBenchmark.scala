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

import java.io.File
import java.util.UUID

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers.{any, anyInt}
import org.mockito.Mockito.{doAnswer, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.{HashPartitioner, ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.{BlockId, BlockManager, DiskBlockManager, DiskBlockObjectWriter, TempShuffleBlockId}
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
object BypassMergeSortShuffleWriterBenchmark extends BenchmarkBase {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency:
    ShuffleDependency[String, String, String] = _

  private var tempDir: File = _
  private val blockIdToFileMap: mutable.Map[BlockId, File] = new mutable.HashMap[BlockId, File]
  private var shuffleHandle: BypassMergeSortShuffleHandle[String, String] = _

  def setup(transferTo: Boolean): BypassMergeSortShuffleWriter[String, String] = {
    MockitoAnnotations.initMocks(this)
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.file.transferTo", String.valueOf(transferTo))
    conf.set("spark.shuffle.file.buffer", "32k")

    if (shuffleHandle == null) {
      shuffleHandle = new BypassMergeSortShuffleHandle[String, String](
        shuffleId = 0,
        numMaps = 1,
        dependency = dependency
      )
    }

    val taskMetrics = new TaskMetrics
    when(dependency.partitioner).thenReturn(new HashPartitioner(10))
    when(dependency.serializer).thenReturn(new JavaSerializer(conf))
    when(dependency.shuffleId).thenReturn(0)

    // Create the temporary directory to write local shuffle and temp files
    tempDir = Utils.createTempDir()
    val outputFile = File.createTempFile("shuffle", null, tempDir)
    // Final mapper data file output
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)

    // Create the temporary writers (backed by files), one for each partition.
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.createTempShuffleBlock()).thenAnswer(
      (invocation: InvocationOnMock) => {
        val blockId = new TempShuffleBlockId(UUID.randomUUID)
        val file = new File(tempDir, blockId.name)
        blockIdToFileMap.put(blockId, file)
        (blockId, file)
      })
    when(blockManager.getDiskWriter(
      any[BlockId],
      any[File],
      any[SerializerInstance],
      anyInt(),
      any[ShuffleWriteMetrics]
    )).thenAnswer(new Answer[DiskBlockObjectWriter] {
      override def answer(invocation: InvocationOnMock): DiskBlockObjectWriter = {
        val args = invocation.getArguments
        val manager = new SerializerManager(new JavaSerializer(conf), conf)
        new DiskBlockObjectWriter(
          args(1).asInstanceOf[File],
          manager,
          args(2).asInstanceOf[SerializerInstance],
          args(3).asInstanceOf[Int],
          syncWrites = false,
          args(4).asInstanceOf[ShuffleWriteMetrics],
          blockId = args(0).asInstanceOf[BlockId]
        )
      }
    })

    // writing the index file
    doAnswer(new Answer[Void] {
      def answer(invocationOnMock: InvocationOnMock): Void = {
        val tmp: File = invocationOnMock.getArguments()(3).asInstanceOf[File]
        if (tmp != null) {
          outputFile.delete
          tmp.renameTo(outputFile)
        }
        null
      }
    }).when(blockResolver)
      .writeIndexFileAndCommit(anyInt, anyInt, any(classOf[Array[Long]]), any(classOf[File]))
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)

    val shuffleWriter = new BypassMergeSortShuffleWriter[String, String](
      blockManager,
      blockResolver,
      shuffleHandle,
      0,
      conf,
      taskMetrics.shuffleWriteMetrics
    )

    shuffleWriter
  }

  def write(writer: BypassMergeSortShuffleWriter[String, String],
            records: Array[(String, String)]): Unit = {
    writer.write(records.iterator)
  }

  def cleanupTempFiles(): Unit = {
    tempDir.delete()
  }

  def writeBenchmarkWithLargeDataset(): Unit = {
    val size = 10000000
    val minNumIters = 10
    val random = new Random(123)
    val data = (1 to size).map { i => {
      val x = random.alphanumeric.take(5).mkString
      Tuple2(x, x)
    } }.toArray
    val benchmark = new Benchmark(
      "BypassMergeSortShuffleWrite (with spill) " + size,
      size,
      minNumIters = minNumIters,
      output = output)
    benchmark.addTimerCase("without transferTo") { timer =>
      val shuffleWriter = setup(false)
      timer.startTiming()
      write(shuffleWriter, data)
      timer.stopTiming()
      cleanupTempFiles()
    }
    benchmark.addTimerCase("with transferTo") { timer =>
      val shuffleWriter = setup(true)
      timer.startTiming()
      write(shuffleWriter, data)
      timer.stopTiming()
      cleanupTempFiles()
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
      size, output = output)
    benchmark.addTimerCase("small dataset without spills on disk") { timer =>
      val shuffleWriter = setup(false)
      timer.startTiming()
      write(shuffleWriter, data)
      timer.stopTiming()
      cleanupTempFiles()
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
