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
import java.util.{LinkedList, UUID}

import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers.{any, anyInt}
import org.mockito.Mockito.{doAnswer, when}
import org.mockito.invocation.InvocationOnMock
import scala.util.Random

import org.apache.spark.{HashPartitioner, ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance, SerializerManager}
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
object UnsafeShuffleWriterBenchmark extends BenchmarkBase {

  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var shuffleBlockResolver:
    IndexShuffleBlockResolver = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency:
    ShuffleDependency[String, String, String] = _
  @Mock(answer = RETURNS_SMART_NULLS) private[sort] val taskContext: TaskContext = null
  @Mock(answer = RETURNS_SMART_NULLS) private[sort] val diskBlockManager: DiskBlockManager = null

  private[sort] val serializer = new KryoSerializer(new SparkConf)
  private[sort] val hashPartitioner = new HashPartitioner(10)
  private[sort] val spillFilesCreated: util.LinkedList[File] = new util.LinkedList[File]
  private var tempDataFile: File = File.createTempFile("test-data", "")
  private var tempDir: File = _
  private var shuffleHandle: SerializedShuffleHandle[String, String] = _
  private var memoryManager: TestMemoryManager = _
  private var taskMemoryManager: TaskMemoryManager = _

  private val DEFAULT_DATA_STRING_SIZE = 5
  private val MIN_NUM_ITERS = 5

  def setup(transferTo: Boolean): UnsafeShuffleWriter[String, String] = {
    MockitoAnnotations.initMocks(this)
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.file.transferTo", String.valueOf(transferTo))
    memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES)
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    if (shuffleHandle == null) {
      shuffleHandle = new SerializedShuffleHandle[String, String](0, 0, dependency)
    }
    val taskMetrics = new TaskMetrics


    tempDir = Utils.createTempDir(null, "test")
    val mergedOutputFile = File.createTempFile("shuffle", "", tempDir)

    // copied from UnsafeShuffleWriterSuite
    // Some tests will override this manager because they change the configuration. This is a
    // default for tests that don't need a specific one.
    val manager: SerializerManager = new SerializerManager(serializer, conf)
    when(blockManager.serializerManager).thenReturn(manager)

    when(blockManager.diskBlockManager).thenReturn(diskBlockManager)
    when(blockManager.getDiskWriter(
      any[BlockId],
      any[File],
      any[SerializerInstance],
      any[Int],
      any[ShuffleWriteMetrics]))
      .thenAnswer((invocationOnMock: InvocationOnMock) => {
        val args = invocationOnMock.getArguments
        new DiskBlockObjectWriter(
          args(1).asInstanceOf[File],
          blockManager.serializerManager,
          args(2).asInstanceOf[SerializerInstance],
          args(3).asInstanceOf[java.lang.Integer],
          false,
          args(4).asInstanceOf[ShuffleWriteMetrics],
          args(0).asInstanceOf[BlockId])
    })

    when(shuffleBlockResolver.getDataFile(anyInt, anyInt)).thenReturn(mergedOutputFile)
    doAnswer((invocationOnMock: InvocationOnMock) => {
        val tmp: File = invocationOnMock.getArguments()(3).asInstanceOf[File]
        mergedOutputFile.delete
        tmp.renameTo(mergedOutputFile)
    }).when(shuffleBlockResolver).writeIndexFileAndCommit(
      any[Int],
      any[Int],
      any[Array[Long]],
      any[File])

    when(diskBlockManager.createTempShuffleBlock)
      .thenAnswer((invocationOnMock: InvocationOnMock) => {
        val blockId: TempShuffleBlockId = new TempShuffleBlockId(UUID.randomUUID)
        val file: File = File.createTempFile("spillFile", ".spill", tempDir)
        spillFilesCreated.add(file)
        (blockId, file)
    })

    when(taskContext.taskMetrics()).thenReturn(taskMetrics)
    when(dependency.serializer).thenReturn(serializer)
    when(dependency.partitioner).thenReturn(hashPartitioner)

    spillFilesCreated.clear()
    new UnsafeShuffleWriter[String, String](
      blockManager,
      shuffleBlockResolver,
      taskMemoryManager,
      shuffleHandle,
      0,
      taskContext,
      conf,
      taskMetrics.shuffleWriteMetrics
    )
  }

  def cleanupTempFiles(): Unit = {
    tempDir.delete()
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
    val benchmark = new Benchmark("UnsafeShuffleWriter with spills",
      size,
      minNumIters = MIN_NUM_ITERS,
      output = output)
    benchmark.addTimerCase("small dataset without spills") { timer =>
      val writer = setup(false)
      val array = createDataInMemory(1000)
      timer.startTiming()
      writer.write(array.iterator)
      timer.stopTiming()
      assert(spillFilesCreated.size() == 1) // The single temp file is for the temp index file
      cleanupTempFiles()
    }
    benchmark.run()
  }

  def writeBenchmarkWithSpill(): Unit = {
    val size = PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES/2/DEFAULT_DATA_STRING_SIZE
    val minNumIters = 5
    createDataOnDisk(size)
    val benchmark = new Benchmark("UnsafeShuffleWriter with spills",
      size,
      minNumIters = minNumIters,
      output = output,
      outputPerIteration = true)
    benchmark.addTimerCase("without transferTo") { timer =>
      val shuffleWriter = setup(false)
      timer.startTiming()
      shuffleWriter.write(DataIterator(inputFile = tempDataFile, DEFAULT_DATA_STRING_SIZE))
      timer.stopTiming()
      assert(spillFilesCreated.size() == 7)
      cleanupTempFiles()
    }
    benchmark.addTimerCase("with transferTo") { timer =>
      val shuffleWriter = setup(false)
      timer.startTiming()
      shuffleWriter.write(DataIterator(inputFile = tempDataFile, DEFAULT_DATA_STRING_SIZE))
      timer.stopTiming()
      assert(spillFilesCreated.size() == 7)
      cleanupTempFiles()
    }
    benchmark.run()
    tempDataFile.delete()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("UnsafeShuffleWriter write") {
      writeBenchmarkWithSmallDataset()
      writeBenchmarkWithSpill()
    }
  }
}
