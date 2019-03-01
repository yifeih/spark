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
import java.nio.channels.ReadableByteChannel
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Mockito.when
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.util.Random

import org.apache.spark.{HashPartitioner, ShuffleDependency, SparkConf, TaskContext}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryManager, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEndpointRef, RpcEnv, RpcEnvFileServer}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.{BlockManager, DiskBlockManager, TempShuffleBlockId}
import org.apache.spark.util.Utils

abstract class ShuffleWriterBenchmarkBase(useKryoSerializer: Boolean) extends BenchmarkBase {

  private val DEFAULT_DATA_STRING_SIZE = 5

  // This is only used in the writer constructors, so it's ok to mock
  @Mock(answer = RETURNS_SMART_NULLS) protected var dependency:
    ShuffleDependency[String, String, String] = _
  // This is only used in the stop() function, so we can safely mock this without affecting perf
  @Mock(answer = RETURNS_SMART_NULLS) protected var taskContext: TaskContext = _

  protected val defaultConf: SparkConf = new SparkConf(loadDefaults = false)
  protected  val serializer: Serializer = if (useKryoSerializer) {
    new KryoSerializer(defaultConf)
  } else {
    new JavaSerializer(defaultConf)
  }
  protected val partitioner: HashPartitioner = new HashPartitioner(10)
  protected val serializerManager: SerializerManager =
    new SerializerManager(serializer, defaultConf)

  private var rpcEnv: RpcEnv = new RpcEnv(defaultConf) {
    override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = { null }
    override def address: RpcAddress = null
    override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = { null }
    override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = { null }
    override def stop(endpoint: RpcEndpointRef): Unit = { }
    override def shutdown(): Unit = { }
    override def awaitTermination(): Unit = { }
    override def deserialize[T](deserializationAction: () => T): T = { deserializationAction() }
    override def fileServer: RpcEnvFileServer = { null }
    override def openChannel(uri: String): ReadableByteChannel = { null }
  }

  protected val tempFilesCreated: ArrayBuffer[File] = new ArrayBuffer[File]
  protected val filenameToFile: mutable.Map[String, File] = new mutable.HashMap[String, File]

  class TestDiskBlockManager(tempDir: File) extends DiskBlockManager(defaultConf, false) {
    override def getFile(filename: String): File = {
      if (filenameToFile.contains(filename)) {
        filenameToFile(filename)
      } else {
        val outputFile = File.createTempFile("shuffle", null, tempDir)
        filenameToFile(filename) = outputFile
        outputFile
      }
    }

    override def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
      var blockId = new TempShuffleBlockId(UUID.randomUUID())
      val file = getFile(blockId)
      tempFilesCreated += file
      (blockId, file)
    }
  }

  class TestBlockManager(tempDir: File, memoryManager: MemoryManager) extends BlockManager("0",
    rpcEnv,
    null,
    serializerManager,
    defaultConf,
    memoryManager,
    null, null, null, null, 1) {
    override val diskBlockManager = new TestDiskBlockManager(tempDir)
  }

  protected var tempDir: File = _
  protected var shuffleMetrics: TaskMetrics = _

  protected var blockManager: BlockManager = _
  protected var blockResolver: IndexShuffleBlockResolver = _

  protected var memoryManager: TestMemoryManager = _
  protected var taskMemoryManager: TaskMemoryManager = _

  MockitoAnnotations.initMocks(this)
  when(dependency.partitioner).thenReturn(partitioner)
  when(dependency.serializer).thenReturn(serializer)
  when(dependency.shuffleId).thenReturn(0)
  shuffleMetrics = new TaskMetrics
  when(taskContext.taskMetrics()).thenReturn(shuffleMetrics)

  def setup(): Unit = {
    memoryManager = new TestMemoryManager(defaultConf)
    memoryManager.limit(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES)
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    tempDir = Utils.createTempDir()
    blockManager = new TestBlockManager(tempDir, memoryManager)
    blockResolver = new IndexShuffleBlockResolver(
      defaultConf,
      blockManager)
  }

  def addBenchmarkCase(benchmark: Benchmark, name: String)(func: Benchmark.Timer => Unit): Unit = {
    benchmark.addTimerCase(name) { timer =>
      setup()
      func(timer)
      teardown()
    }
  }

  def teardown(): Unit = {
    FileUtils.deleteDirectory(tempDir)
    tempFilesCreated.clear()
    filenameToFile.clear()
  }

  protected class DataIterator private (
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

  protected object DataIterator {
    def apply(inputFile: File, bufferSize: Int): DataIterator = {
      val inputStream = new BufferedInputStream(
        new FileInputStream(inputFile), DEFAULT_DATA_STRING_SIZE)
      val buffer = new Array[Byte](DEFAULT_DATA_STRING_SIZE)
      new DataIterator(inputStream, buffer)
    }
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
    val tempDataFile = File.createTempFile("test-data", "")
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

}
