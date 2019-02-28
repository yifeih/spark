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

import java.io.{File, FileOutputStream, OutputStream}
import java.util.concurrent.{Callable, Executors}

import com.google.common.io.CountingOutputStream
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Answers.RETURNS_SMART_NULLS
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import scala.util.Random

import org.apache.spark.{MapOutputTracker, ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.FileSegmentManagedBuffer
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}
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
object BlockStoreShuffleReaderBenchmark extends BenchmarkBase {


  @Mock(answer = RETURNS_SMART_NULLS) private var blockManager: BlockManager = _
  @Mock(answer = RETURNS_SMART_NULLS) private var transferService: BlockTransferService = _
  @Mock(answer = RETURNS_SMART_NULLS) private var dependency:
    ShuffleDependency[String, String, String] = _
  @Mock(answer = RETURNS_SMART_NULLS) private var taskContext: TaskContext = _
  @Mock(answer = RETURNS_SMART_NULLS) private var mapOutputTracker: MapOutputTracker = _

  private val defaultConf: SparkConf = new SparkConf()
    .set("spark.shuffle.compress", "false")
    .set("spark.shuffle.spill.compress", "false")
  private val serializer: KryoSerializer = new KryoSerializer(defaultConf)
  private val serializerManager: SerializerManager = new SerializerManager(serializer, defaultConf)
  private val execBlockManagerId: BlockManagerId = BlockManagerId("execId", "host", 8000)
  private val remoteBlockManagerId: BlockManagerId = BlockManagerId("remote", "remote", 8000)
  private val transportConf: TransportConf =
    SparkTransportConf.fromSparkConf(defaultConf, "shuffle")

  private var tempDir: File = _

  private val SHUFFLE_ID: Int = 0
  private val REDUCE_ID: Int = 0
  private val NUM_MAPS: Int = 1

  private val DEFAULT_DATA_STRING_SIZE = 5
  private val executorPool = Executors.newFixedThreadPool(10)


  def setup(size: Int, fetchLocal: Boolean): BlockStoreShuffleReader[String, String] = {
    MockitoAnnotations.initMocks(this)
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

    val shuffleHandle = new BaseShuffleHandle(
      shuffleId = SHUFFLE_ID,
      numMaps = NUM_MAPS,
      dependency = dependency)

    val taskMetrics = new TaskMetrics
    when(taskContext.taskMetrics()).thenReturn(taskMetrics)

    when(blockManager.shuffleClient).thenReturn(transferService)
    when(dependency.serializer).thenReturn(serializer)
    when(blockManager.blockManagerId).thenReturn(execBlockManagerId)
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
      when(transferService.fetchBlocks(
        any[String],
        any[Int],
        any[String],
        any[Array[String]],
        any[BlockFetchingListener],
        any[DownloadFileManager]
      )).thenAnswer((invocation: InvocationOnMock) => {
        val blocks = invocation.getArguments()(3).asInstanceOf[Array[String]]
        val listener = invocation.getArguments()(4).asInstanceOf[BlockFetchingListener]

        // TODO: do this in parallel?
        for (blockId <- blocks) {
          val generatedFile = generateDataOnDisk(size)
          listener.onBlockFetchSuccess(blockId, new FileSegmentManagedBuffer(
            transportConf,
            generatedFile._1,
            0,
            generatedFile._2
          ))
//          executorPool.submit(new Callable[Unit] {
//            override def call(): Unit = {
//              val fileGenerated = generateDataOnDisk(size)
//              listener.onBlockFetchSuccess(blockId, new FileSegmentManagedBuffer(
//                transportConf,
//                fileGenerated,
//                0,
//                size.toLong * DEFAULT_DATA_STRING_SIZE
//              ))
//            }
//          })
        }
      })
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
