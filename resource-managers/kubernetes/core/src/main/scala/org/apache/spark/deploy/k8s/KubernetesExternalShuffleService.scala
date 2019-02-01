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

package org.apache.spark.deploy.k8s

import java.io.{DataOutputStream, File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.file.Paths
import java.util
import java.util.concurrent.{ConcurrentHashMap, ExecutionException, TimeUnit}
import java.util.function.BiFunction

import com.codahale.metrics._
import com.google.common.cache.{CacheBuilder, CacheLoader, Weigher}
import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.ExternalShuffleService
import org.apache.spark.deploy.k8s.Config.KUBERNETES_REMOTE_SHUFFLE_SERVICE_CLEANUP_INTERVAL
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.FileSegmentManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.shuffle._
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.network.util.{JavaUtils, TransportConf}
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * An RPC endpoint that receives registration requests from Spark drivers running on Kubernetes.
 * It detects driver termination and calls the cleanup callback to [[ExternalShuffleService]].
 */
private[spark] class KubernetesExternalShuffleBlockHandler(
    transportConf: TransportConf,
    cleanerIntervals: Long,
    indexCacheSize: String)
  extends ExternalShuffleBlockHandler(transportConf, null) with Logging {

  ThreadUtils.newDaemonSingleThreadScheduledExecutor("shuffle-cleaner-watcher")
    .scheduleAtFixedRate(new CleanerThread(), 0, cleanerIntervals, TimeUnit.SECONDS)

  // Stores a map of app id to app state (timeout value and last heartbeat)
  private val connectedApps = new ConcurrentHashMap[String, AppState]()
  private val indexCacheLoader = new CacheLoader[File, ShuffleIndexInformation]() {
    override def load(file: File): ShuffleIndexInformation = new ShuffleIndexInformation(file)
  }
  private val shuffleIndexCache = CacheBuilder.newBuilder()
    .maximumWeight(JavaUtils.byteStringAsBytes(indexCacheSize))
    .weigher(new Weigher[File, ShuffleIndexInformation]() {
      override def weigh(file: File, indexInfo: ShuffleIndexInformation): Int =
        indexInfo.getSize
    })
    .build(indexCacheLoader)

  // TODO: Investigate cleanup if appId is terminated
  private val globalPartitionLengths = new ConcurrentHashMap[(String, Int, Int), TreeMap[Int, Long]]

  private final val shuffleDir = Utils.createDirectory("/tmp", "spark-shuffle-dir")

  private final val metricSet: RemoteShuffleMetrics = new RemoteShuffleMetrics()

  private def scanLeft[a, b](xs: Iterable[a])(s: b)(f: (b, a) => b) =
    xs.foldLeft(List(s))( (acc, x) => f(acc.head, x) :: acc).reverse

  protected override def handleMessage(
      message: BlockTransferMessage,
      client: TransportClient,
      callback: RpcResponseCallback): Unit = {
    message match {
      case RegisterDriverParam(appId, appState) =>
        val responseDelayContext = metricSet.registerDriverRequestLatencyMillis.time()
        val address = client.getSocketAddress
        val timeout = appState.heartbeatTimeout
        logInfo(s"Received registration request from app $appId (remote address $address, " +
          s"heartbeat timeout $timeout ms).")
        if (connectedApps.containsKey(appId)) {
          logWarning(s"Received a registration request from app $appId, but it was already " +
            s"registered")
        }
        val driverDir = Paths.get(shuffleDir.getAbsolutePath, appId).toFile
        if (!driverDir.mkdir()) {
          throw new RuntimeException(s"Failed to create dir ${driverDir.getAbsolutePath}")
        }
        connectedApps.put(appId, appState)
        responseDelayContext.stop()
        callback.onSuccess(ByteBuffer.allocate(0))

      case Heartbeat(appId) =>
        val address = client.getSocketAddress
        Option(connectedApps.get(appId)) match {
          case Some(existingAppState) =>
            logTrace(s"Received ShuffleServiceHeartbeat from app '$appId' (remote " +
              s"address $address).")
            existingAppState.lastHeartbeat = System.nanoTime()
          case None =>
            logWarning(s"Received ShuffleServiceHeartbeat from an unknown app (remote " +
              s"address $address, appId '$appId').")
        }

      case RegisterIndexParam(appId, shuffleId, mapId) =>
        logInfo(s"Received register index param from app $appId")
        globalPartitionLengths.putIfAbsent(
          (appId, shuffleId, mapId), TreeMap.empty[Int, Long])
        callback.onSuccess(ByteBuffer.allocate(0))

      case UploadIndexParam(appId, shuffleId, mapId) =>
        val responseDelayContext = metricSet.writeIndexRequestLatencyMillis.time()
        try {
          logInfo(s"Received upload index param from app $appId")
          val partitionMap = globalPartitionLengths.get((appId, shuffleId, mapId))
          val out = new DataOutputStream(
            new FileOutputStream(getFile(appId, shuffleId, mapId, "index")))
          scanLeft(partitionMap.values)(0L)(_ + _).foreach(l => out.writeLong(l))
          out.close()
          callback.onSuccess(ByteBuffer.allocate(0))
        } finally {
          val nanoSeconds = responseDelayContext.stop()
          logInfo("METRICS: UploadIndexParam processing time: " + nanoSeconds)
        }

      case OpenParam(appId, shuffleId, mapId, partitionId) =>
        logInfo(s"Received open param from app $appId")
        val responseDelayContext = metricSet.openBlockRequestLatencyMillis.time()
        val indexFile = getFile(appId, shuffleId, mapId, "index")
        logInfo(s"Map: " +
          s"${globalPartitionLengths.get((appId, shuffleId, mapId)).toString()}" +
          s"for partitionId: $partitionId")
        try {
          val shuffleIndexInformation = shuffleIndexCache.get(indexFile)
          val shuffleIndexRecord = shuffleIndexInformation.getIndex(partitionId)
          val managedBuffer = new FileSegmentManagedBuffer(
            transportConf,
            getFile(appId, shuffleId, mapId, "data"),
            shuffleIndexRecord.getOffset,
            shuffleIndexRecord.getLength)
          callback.onSuccess(managedBuffer.nioByteBuffer())
        } catch {
          case e: ExecutionException => logError(s"Unable to write index file $indexFile", e)
        } finally {
          responseDelayContext.stop()
        }
      case _ => super.handleMessage(message, client, callback)
    }
  }

  protected override def handleStream(
    header: BlockTransferMessage,
    client: TransportClient,
    callback: RpcResponseCallback): StreamCallbackWithID = {
    header match {
      case UploadParam(
          appId, shuffleId, mapId, partitionId, partitionLength) =>
        val responseDelayContext = metricSet.writeBlockRequestLatencyMillis.time()
        try {
          logInfo(s"Received upload param from app $appId")
          val lengthMap = TreeMap(partitionId -> partitionLength.toLong)
          globalPartitionLengths.merge((appId, shuffleId, mapId), lengthMap,
            new BiFunction[TreeMap[Int, Long], TreeMap[Int, Long], TreeMap[Int, Long]]() {
              override def apply(t: TreeMap[Int, Long], u: TreeMap[Int, Long]):
                  TreeMap[Int, Long] = {
                t ++ u
              }
            })
          getFileWriterStreamCallback(
            appId, shuffleId, mapId, "data", FileWriterStreamCallback.FileType.DATA)
        } finally {
          val nanoSeconds = responseDelayContext.stop()
          logInfo("METRICS: OpenStream processing time: " + nanoSeconds)
        }
      case _ =>
        super.handleStream(header, client, callback)
    }
  }

  protected override def getAllMetrics: MetricSet = metricSet

  private def getFileWriterStreamCallback(
      appId: String,
      shuffleId: Int,
      mapId: Int,
      extension: String,
      fileType: FileWriterStreamCallback.FileType): StreamCallbackWithID = {
    val file = getFile(appId, shuffleId, mapId, extension)
    val streamCallback =
      new FileWriterStreamCallback(appId, shuffleId, mapId, file, fileType)
    streamCallback.open()
    streamCallback
  }

  private def getFile(
      appId: String,
      shuffleId: Int,
      mapId: Int,
      extension: String): File = {
    Paths.get(shuffleDir.getAbsolutePath, appId,
      s"shuffle_${shuffleId}_${mapId}_0.$extension").toFile
  }

  /** An extractor object for matching BlockTransferMessages. */
  private object RegisterDriverParam {
    def unapply(r: RegisterDriver): Option[(String, AppState)] =
      Some((r.getAppId, new AppState(r.getHeartbeatTimeoutMs, System.nanoTime())))
  }

  private object Heartbeat {
    def unapply(h: ShuffleServiceHeartbeat): Option[String] = Some(h.getAppId)
  }

  private object UploadParam {
    def unapply(u: UploadShufflePartitionStream): Option[(String, Int, Int, Int, Int)] =
      Some((u.appId, u.shuffleId, u.mapId, u.partitionId, u.partitionLength))
  }

  private object UploadIndexParam {
    def unapply(u: UploadShuffleIndex): Option[(String, Int, Int)] =
      Some((u.appId, u.shuffleId, u.mapId))
  }

  private object RegisterIndexParam {
    def unapply(u: RegisterShuffleIndex): Option[(String, Int, Int)] =
      Some((u.appId, u.shuffleId, u.mapId))
  }

  private object OpenParam {
    def unapply(o: OpenShufflePartition): Option[(String, Int, Int, Int)] =
      Some((o.appId, o.shuffleId, o.mapId, o.partitionId))
  }

  private class AppState(val heartbeatTimeout: Long, @volatile var lastHeartbeat: Long)

  private class CleanerThread extends Runnable {
    override def run(): Unit = {
      val now = System.nanoTime()
      connectedApps.asScala.foreach { case (appId, appState) =>
        if (now - appState.lastHeartbeat > appState.heartbeatTimeout * 1000 * 1000) {
          logInfo(s"Application $appId timed out. Removing shuffle files.")
          connectedApps.remove(appId)
          applicationRemoved(appId, false)
          try {
            val driverDir = Paths.get(shuffleDir.getAbsolutePath, appId).toFile
            logInfo(s"Driver dir is: ${driverDir.getAbsolutePath}")
            driverDir.delete()
          } catch {
            case e: Exception => logError("Unable to delete files", e)
          }
        }
      }
    }
  }
  private class RemoteShuffleMetrics extends MetricSet {
    private val allMetrics = new util.HashMap[String, Metric]()
    // Time latency for write request in ms
    private val _writeBlockRequestLatencyMillis = new Timer()
    def writeBlockRequestLatencyMillis: Timer = _writeBlockRequestLatencyMillis
    // Time latency for write index file in ms
    private val _writeIndexRequestLatencyMillis = new Timer()
    def writeIndexRequestLatencyMillis: Timer = _writeIndexRequestLatencyMillis
    // Time latency for read request in ms
    private val _openBlockRequestLatencyMillis = new Timer()
    def openBlockRequestLatencyMillis: Timer = _openBlockRequestLatencyMillis
    // Time latency for executor registration latency in ms
    private val _registerDriverRequestLatencyMillis = new Timer()
    def registerDriverRequestLatencyMillis: Timer = _registerDriverRequestLatencyMillis
    // Block transfer rate in byte per second
    private val _blockTransferRateBytes = new Meter()
    def blockTransferRateBytes: Meter = _blockTransferRateBytes

    allMetrics.put("writeBlockRequestLatencyMillis", _writeBlockRequestLatencyMillis)
    allMetrics.put("writeIndexRequestLatencyMillis", _writeIndexRequestLatencyMillis)
    allMetrics.put("openBlockRequestLatencyMillis", _openBlockRequestLatencyMillis)
    allMetrics.put("registerDriverRequestLatencyMillis", _registerDriverRequestLatencyMillis)
    allMetrics.put("blockTransferRateBytes", _blockTransferRateBytes)
    override def getMetrics: util.Map[String, Metric] = allMetrics
  }

}

/**
 * A wrapper of [[ExternalShuffleService]] that provides an additional endpoint for drivers
 * to associate with. This allows the shuffle service to detect when a driver is terminated
 * and can clean up the associated shuffle files.
 */
private[spark] class KubernetesExternalShuffleService(
    conf: SparkConf, securityManager: SecurityManager)
  extends ExternalShuffleService(conf, securityManager) {

  protected override def newShuffleBlockHandler(
      conf: TransportConf): ExternalShuffleBlockHandler = {
    val cleanerIntervals = this.conf.get(KUBERNETES_REMOTE_SHUFFLE_SERVICE_CLEANUP_INTERVAL)
    val indexCacheSize = this.conf.get("spark.shuffle.service.index.cache.size", "100m")
    new KubernetesExternalShuffleBlockHandler(conf, cleanerIntervals, indexCacheSize)
  }
}

private[spark] object KubernetesExternalShuffleService extends Logging {

  def main(args: Array[String]): Unit = {
    ExternalShuffleService.main(args,
      (conf: SparkConf, sm: SecurityManager) => new KubernetesExternalShuffleService(conf, sm))
  }
}


