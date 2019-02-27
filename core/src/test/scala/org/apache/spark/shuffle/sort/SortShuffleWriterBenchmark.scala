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

import java.io.{BufferedInputStream, File, FileInputStream, FileOutputStream}
import java.util
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers.{any, anyInt}
import org.mockito.Mockito.{doAnswer, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.{Aggregator, HashPartitioner, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.{BaseShuffleHandle, IndexShuffleBlockResolver}
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
object SortShuffleWriterBenchmark extends BenchmarkBase {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var blockResolver: IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var diskBlockManager: DiskBlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency:
    ShuffleDependency[String, String, String] = _

  private var tempDir: File = _
  private var shuffleHandle: BaseShuffleHandle[String, String, String] = _
  private val spillFilesCreated: util.LinkedList[File] = new util.LinkedList[File]
  private val partitioner: HashPartitioner = new HashPartitioner(10)
  private val defaultConf: SparkConf = new SparkConf()
  private val serializer = new KryoSerializer(defaultConf)
  private val serializerManager = new SerializerManager(serializer, defaultConf)
  private var memoryManager: TestMemoryManager = new TestMemoryManager(defaultConf)
  private var taskMemoryManager: TaskMemoryManager = new TaskMemoryManager(memoryManager, 0)

  private val DEFAULT_DATA_STRING_SIZE = 5
  private val MIN_NUM_ITERS = 5

  def setup(aggregator: Option[Aggregator[String, String, String]],
            sorter: Option[Ordering[String]]): SortShuffleWriter[String, String, String] = {
    MockitoAnnotations.initMocks(this)
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
    shuffleHandle = new BaseShuffleHandle(
      shuffleId = 0,
      numMaps = 1,
      dependency = dependency)

    when(dependency.partitioner).thenReturn(partitioner)
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.shuffleId).thenReturn(0)
    if (aggregator.isEmpty && sorter.isEmpty) {
      when(dependency.mapSideCombine).thenReturn(false)
    } else {
      when(dependency.mapSideCombine).thenReturn(false)
      when(dependency.aggregator).thenReturn(aggregator)
      when(dependency.keyOrdering).thenReturn(sorter)
    }

    tempDir = Utils.createTempDir()
    val outputFile = File.createTempFile("shuffle", null, tempDir)
    when(blockResolver.getDataFile(0, 0)).thenReturn(outputFile)
    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(diskBlockManager.createTempShuffleBlock()).thenAnswer(
      (invocation: InvocationOnMock) => {
        val blockId = new TempShuffleBlockId(UUID.randomUUID)
        val file = new File(tempDir, blockId.name)
        // scalastyle:off println
        println("created spill file")
        spillFilesCreated.add(file)
        // scalastyle:on println
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
        val manager = serializerManager
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

    val taskMetrics = new TaskMetrics
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)

    memoryManager.limit(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES)
    when(taskContext.taskMemoryManager()).thenReturn(taskMemoryManager)

    val shuffleWriter = new SortShuffleWriter[String, String, String](
      blockResolver,
      shuffleHandle,
      0,
      taskContext
    )
    shuffleWriter
  }

  def cleanupTempFiles(): Unit = {
    FileUtils.deleteDirectory(tempDir)
  }

  def createDataInMemory(size: Int): Array[(String, String)] = {
    val random = new Random(123)
    (1 to size).map { i => {
      val x = random.alphanumeric.take(DEFAULT_DATA_STRING_SIZE).mkString
      Tuple2(x, x)
    } }.toArray
  }

  def createDataOnDisk(size: Int): File = {
    // scalastyle:off println
    val tempDataFile: File = File.createTempFile("test-data", "")
    println("Generating test data with num records: " + size)
    val random = new Random(123)
    val dataOutput = new FileOutputStream(tempDataFile)
    try {
      (1 to size).foreach { i => {
        if (i % 1000000 == 0) {
          println("Wrote " + i + " test data points")
        }
        val x = random.alphanumeric.take(DEFAULT_DATA_STRING_SIZE).mkString
        dataOutput.write(x.getBytes)
      }}
    }
    finally {
      dataOutput.close()
    }
    tempDataFile
    // scalastyle:off println
  }

  private class DataIterator private (
    private val inputStream: BufferedInputStream,
    private val buffer: Array[Byte]) extends Iterator[Product2[String, String]] {
    override def hasNext: Boolean = {
      inputStream.available() > 0
    }

    override def next(): Product2[String, String] = {
      val read = inputStream.read(buffer)
      assert(read == buffer.length)
      val string = buffer.mkString
      (string, string)
    }
  }

  private object DataIterator {
    def apply(inputFile: File, bufferSize: Int): DataIterator = {
      val inputStream = new BufferedInputStream(
        new FileInputStream(inputFile), DEFAULT_DATA_STRING_SIZE)
      val buffer = new Array[Byte](DEFAULT_DATA_STRING_SIZE)
      new DataIterator(inputStream, buffer)
    }
  }

  def writeBenchmarkWithSmallDataset(): Unit = {
    val size = 1000
    val benchmark = new Benchmark("SortShuffleWriter with spills",
      size,
      minNumIters = MIN_NUM_ITERS,
      output = output)
    benchmark.addTimerCase("small dataset without spills") { timer =>
      val writer = setup(Option.empty, Option.empty)
      val array = createDataInMemory(1000)
      timer.startTiming()
      writer.write(array.iterator)
      timer.stopTiming()
      assert(spillFilesCreated.size() == 0)
      cleanupTempFiles()
    }
    benchmark.run()
  }

  def writeBenchmarkWithSpill(dataFile: File, size: Int): Unit = {
    val benchmark = new Benchmark("SortShuffleWriter with spills",
      size,
      minNumIters = MIN_NUM_ITERS,
      output = output,
      outputPerIteration = true)
    benchmark.addTimerCase("no map side combine") { timer =>
      val shuffleWriter = setup(Option.empty, Option.empty)
      timer.startTiming()
      shuffleWriter.write(DataIterator(inputFile = dataFile, DEFAULT_DATA_STRING_SIZE))
      timer.stopTiming()
      assert(spillFilesCreated.size() > 0)
      cleanupTempFiles()
    }

    def createCombiner(i: String): String = i
    def mergeValue(i: String, j: String): String = if (Ordering.String.compare(i, j) > 0) i else j
    def mergeCombiners(i: String, j: String): String =
      if (Ordering.String.compare(i, j) > 0) i else j
    val aggregator =
      new Aggregator[String, String, String](createCombiner, mergeValue, mergeCombiners)
    benchmark.addTimerCase("with map side aggregation") { timer =>
      val shuffleWriter = setup(Some(aggregator), Option.empty)
      timer.startTiming()
      shuffleWriter.write(DataIterator(inputFile = dataFile, DEFAULT_DATA_STRING_SIZE))
      timer.stopTiming()
      assert(spillFilesCreated.size() > 0)
      cleanupTempFiles()
    }

    val sorter = Ordering.String
    benchmark.addTimerCase("with map side sort") { timer =>
      val shuffleWriter = setup(Option.empty, Some(sorter))
      timer.startTiming()
      shuffleWriter.write(DataIterator(inputFile = dataFile, DEFAULT_DATA_STRING_SIZE))
      timer.stopTiming()
      assert(spillFilesCreated.size() > 0)
      cleanupTempFiles()
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    runBenchmark("SortShuffleWriter writer") {
      writeBenchmarkWithSmallDataset()
      val size = PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES/4/DEFAULT_DATA_STRING_SIZE
      val tempDataFile = createDataOnDisk(size)
      writeBenchmarkWithSpill(tempDataFile, size)
      tempDataFile.delete()
    }
  }
}
