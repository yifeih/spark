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
package org.apache.spark.shuffle

import java.io.{File, FileOutputStream}
import java.nio.channels.ReadableByteChannel
import java.util.concurrent.Executors

import com.google.common.io.CountingOutputStream
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers._
import org.mockito.Mockito.when
import org.mockito.{Mock, MockitoAnnotations}
import scala.concurrent.Future
import scala.util.Random

import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryManager, TaskMemoryManager, TestMemoryManager}
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEndpointRef, RpcEnv, RpcEnvFileServer}
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, BlockManagerMaster, ShuffleBlockId}
import org.apache.spark.util.Utils
import org.apache.spark.{MapOutputTracker, ShuffleDependency, SparkConf, SparkEnv, TaskContext}

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
object BlockStoreShuffleReaderBenchmark extends BenchmarkBase {

  // this is only used to retrieve info about the aggregator/sorters/serializers,
  // so it shouldn't affect the performance significantly
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency:
    ShuffleDependency[String, String, String] = _
  // used only to get metrics, so does not affect perf significantly
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  // only used to retrieve info about the maps at the beginning, doesn't affect perf significantly
  @Mock(answer = RETURNS_SMART_NULLS) private var mapOutputTracker: MapOutputTracker = _
  // this is only used when initializing the block manager, so doesn't affect perf
  @Mock(answer = RETURNS_SMART_NULLS) private var blockManagerMaster: BlockManagerMaster = _

  private val defaultConf: SparkConf = new SparkConf()
    .set("spark.shuffle.compress", "false")
    .set("spark.shuffle.spill.compress", "false")
    .set("spark.authenticate", "false")
    .set("spark.app.id", "test-app")
  private val serializer: KryoSerializer = new KryoSerializer(defaultConf)
  private val serializerManager: SerializerManager = new SerializerManager(serializer, defaultConf)
  private val execBlockManagerId: BlockManagerId = BlockManagerId("localhost", "localhost", 7000)
  private val remoteBlockManagerId: BlockManagerId = BlockManagerId("localhost", "localhost", 7002)
  private val transportConf: TransportConf =
    SparkTransportConf.fromSparkConf(defaultConf, "shuffle")

  private var tempDir: File = _

  private val SHUFFLE_ID: Int = 0
  private val REDUCE_ID: Int = 0
  private val NUM_MAPS: Int = 1

  private val DEFAULT_DATA_STRING_SIZE = 5

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

  protected var memoryManager: TestMemoryManager = _
  protected var taskMemoryManager: TaskMemoryManager = _

  class TestBlockManager(memoryManager: MemoryManager,
                         transferService: BlockTransferService,
                         blockManagerMaster: BlockManagerMaster,
                         dataFile: File,
                         fileLength: Long) extends BlockManager("0",
    rpcEnv,
    blockManagerMaster,
    serializerManager,
    defaultConf,
    memoryManager,
    null, null, transferService, null, 1) {
    blockManagerId = execBlockManagerId

    override def getBlockData(blockId: BlockId): ManagedBuffer = {
      new FileSegmentManagedBuffer(
        transportConf,
        dataFile,
        0,
        fileLength
      )
    }
  }

  private var blockManager : BlockManager = _
  private val securityManager = new org.apache.spark.SecurityManager(defaultConf)


  def setup(size: Int, fetchLocal: Boolean): BlockStoreShuffleReader[String, String] = {
    MockitoAnnotations.initMocks(this)
    when(blockManagerMaster.registerBlockManager(
      any[BlockManagerId], any[Long], any[Long], any[RpcEndpointRef])).thenReturn(null)
    val dataFileAndLength = generateDataOnDisk(10)

    memoryManager = new TestMemoryManager(defaultConf)
    taskMemoryManager = new TaskMemoryManager(memoryManager, 0)
    val localShuffleClient = new NettyBlockTransferService(
      defaultConf,
      new org.apache.spark.SecurityManager(defaultConf),
      "localhost",
      "localhost",
        7000,
        1
    )
    val blockManager =
      new TestBlockManager(memoryManager,
        localShuffleClient,
        blockManagerMaster,
        dataFileAndLength._1,
        dataFileAndLength._2)
    blockManager.initialize(defaultConf.getAppId)

    val externalServer = new NettyBlockTransferService(
      defaultConf,
      new org.apache.spark.SecurityManager(defaultConf),
      "localhost",
      "localhost",
      7002,
      1
    )

    val externalBlockManager = new TestBlockManager(
      memoryManager,
      externalServer,
      blockManagerMaster,
      dataFileAndLength._1,
      dataFileAndLength._2)
    externalBlockManager.initialize(defaultConf.getAppId)

    SparkEnv.set(new SparkEnv(
      "0",
      null,
      serializer,
      null,
      serializerManager,
      mapOutputTracker,
      null,
      null,
      blockManager,
      null,
      null,
      null,
      null,
      defaultConf
    ))

    val shuffleHandle = new BaseShuffleHandle(
      shuffleId = SHUFFLE_ID,
      numMaps = NUM_MAPS,
      dependency = dependency)

    val taskMetrics = new TaskMetrics
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)

    when(dependency.serializer).thenReturn(serializer)
    when(mapOutputTracker.getMapSizesByExecutorId(SHUFFLE_ID, REDUCE_ID, REDUCE_ID + 1))
      .thenReturn {
        val shuffleBlockIdsAndSizes = (0 until NUM_MAPS).map { mapId =>
          val shuffleBlockId = ShuffleBlockId(SHUFFLE_ID, mapId, REDUCE_ID)
          (shuffleBlockId, DEFAULT_DATA_STRING_SIZE * size.toLong)
        }
        // TODO: configurable
        Seq((remoteBlockManagerId, shuffleBlockIdsAndSizes)).toIterator
      }

    tempDir = Utils.createTempDir(null, "shuffle")

    if (fetchLocal) {
      // to do
    } else {
    }

    // TODO: use aggregation + sort
    when(dependency.aggregator).thenReturn(Option.empty)
    when(dependency.keyOrdering).thenReturn(Option.empty)

    new BlockStoreShuffleReader[String, String](
      shuffleHandle,
      0,
      1,
      taskContext,
      taskMetrics.createTempShuffleReadMetrics(),
      serializerManager,
      blockManager,
      mapOutputTracker
    )
  }

  def generateDataOnDisk(size: Int): (File, Long) = {
    // scalastyle:off println
    val tempDataFile: File = File.createTempFile("test-data", "", tempDir)
    println("Generating test data with num records: " + size)
    val random = new Random(123)
    val dataOutput = new FileOutputStream(tempDataFile)
    val coutingOutput = new CountingOutputStream(dataOutput)
    val serializedOutput = serializer.newInstance().serializeStream(coutingOutput)
    try {
      (1 to size).foreach { i => {
        if (i % 1000000 == 0) {
          println("Wrote " + i + " test data points")
        }
        val x = random.alphanumeric.take(DEFAULT_DATA_STRING_SIZE).mkString
        serializedOutput.writeKey(x)
        serializedOutput.writeValue(x)
      }}
    }
    finally {
      serializedOutput.close()
    }
    (tempDataFile, coutingOutput.getCount)
    // scalastyle:off println
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val reader = setup(10, false)
    // scalastyle:off println
    println(reader.read().length)
    // scalastyle:on println
//    assert(reader.read().length == 10 * NUM_MAPS)

    runBenchmark("SortShuffleWriter writer") {
      // todo
    }
  }
}
