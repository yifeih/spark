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

// scalastyle:off println

package org.apache.spark.examples

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerTaskEnd

class ShuffleMetricsOutputSparkListener extends SparkListener with Logging {


  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskMetrics = taskEnd.taskMetrics
    logInfo(s"TaskMetrics: " +
      s"${taskEnd.stageId} " +
      s"${taskMetrics.executorRunTime} " +
      s"${taskMetrics.executorCpuTime} " +
      s"${taskMetrics.jvmGCTime} " +
      s"${taskMetrics.resultSize} " +
      s"${taskMetrics.executorDeserializeTime} " +
      s"${taskMetrics.executorDeserializeCpuTime} " +
      s"${taskMetrics.peakExecutionMemory} ")

    val readMetrics = taskMetrics.shuffleReadMetrics
    logInfo(s"ReadMetrics: " +
      s"${readMetrics.totalBytesRead} " +
      s"${readMetrics.recordsRead}")

    val writeMetrics = taskMetrics.shuffleWriteMetrics
    logInfo(s"WriteMetrics: " +
      s"${writeMetrics.writeTime} " +
      s"${writeMetrics.bytesWritten} " +
      s"${writeMetrics.recordsWritten} " +
      s"${writeMetrics.fileWriteTime} " +
      s"${writeMetrics.numFilesWritten}")

    logInfo(s"WriteMetrics-writeTime: " + writeMetrics.writeTime)
    logInfo(s"WriteMetrics-fileWriteTime: " + writeMetrics.fileWriteTime)
    logInfo(s"WriteMetrics-numFilesWritten: " + writeMetrics.numFilesWritten)
    logInfo(s"WriteMetrics-indexFileWriteTime: " + writeMetrics.indexFileWriteTime)
    logInfo(s"WriteMetrics-streamCopyWriteTime: " + writeMetrics.streamCopyWriteTime)
    logInfo(s"WriteMetrics-bytesWritten: " + writeMetrics.bytesWritten)
    logInfo(s"WriteMetrics-recordsWritten: " + writeMetrics.recordsWritten)
    logInfo(s"WriteMetrics-createPartitionWriterTime: " + writeMetrics.createPartitionWriterTime)
    logInfo(s"WriteMetrics-streamFileWriteTime: " + writeMetrics.streamFileWriteTime)
    logInfo(s"WriteMetrics-createMapOutputWriterTime: " + writeMetrics.createMapOutputWriterTime)
    logInfo(s"WriteMetrics-fileInputStreamTime: " + writeMetrics.fileInputStreamTime)
    logInfo(s"WriteMetrics-createClientFactoryTime: " + writeMetrics.createClientFactoryTime)
    logInfo(s"WriteMetrics-createClientTime: " + writeMetrics.createClientTime)
  }

}


// scalastyle:on println