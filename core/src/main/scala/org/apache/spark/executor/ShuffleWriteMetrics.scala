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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends ShuffleWriteMetricsReporter with Serializable {
  private[executor] val _bytesWritten = new LongAccumulator
  private[executor] val _recordsWritten = new LongAccumulator
  private[executor] val _writeTime = new LongAccumulator
  private[executor] val _fileWriteTime = new LongAccumulator
  private[executor] val _numFilesWritten = new LongAccumulator
  private[executor] val _indexFileWriteTime = new LongAccumulator
  private[executor] val _streamCopyWriteTime = new LongAccumulator
  private[executor] val _createPartitionWriterTime = new LongAccumulator
  private[executor] val _createMapOutputWriterTime = new LongAccumulator
  private[executor] val _streamFileWriteTime = new LongAccumulator
  private[executor] val _fileInputStreamTime = new LongAccumulator
  private[executor] val _createClientFactoryTime = new LongAccumulator
  private[executor] val _createClientTime = new LongAccumulator

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.sum

  /**
   * Time it takes to write the file partition files.
   */
  def fileWriteTime: Long = _fileWriteTime.sum

  /**
   * Number of files written.
   */
  def numFilesWritten: Long = _numFilesWritten.sum

  /**
   * Time it takes to write the index file.
   */
  def indexFileWriteTime: Long = _indexFileWriteTime.sum

  /**
   * Time it takes to stream data.
   */
  def streamCopyWriteTime: Long = _streamCopyWriteTime.sum

  def createPartitionWriterTime: Long = _createPartitionWriterTime.sum

  def createMapOutputWriterTime: Long = _createMapOutputWriterTime.sum

  def streamFileWriteTime: Long = _streamFileWriteTime.sum

  def fileInputStreamTime: Long = _fileInputStreamTime.sum

  def createClientFactoryTime: Long = _createClientFactoryTime.sum

  def createClientTime: Long = _createClientTime.sum

  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)
  private[spark] override def incFileWriteTime(v: Long): Unit = _fileWriteTime.add(v)
  private[spark] override def incNumFilesWritten(): Unit = _numFilesWritten.add(1)
  private[spark] override def incIndexFileWriteTime(v: Long): Unit = _indexFileWriteTime.add(v)
  private[spark] override def incStreamCopyWriteTime(v: Long): Unit = _streamCopyWriteTime.add(v)
  override private[spark] def incCreatePartitionWriterTime(v: Long): Unit =
    _createPartitionWriterTime.add(v)
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] override def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }

  override private[spark] def incCreateMapOutputWriterTime(v: Long): Unit =
    _createMapOutputWriterTime.add(v)

  override private[spark] def incStreamFileWriteTime(v: Long): Unit = _streamFileWriteTime.add(v)

  override private[spark] def incFileInputStreamTime(v: Long): Unit = _fileInputStreamTime.add(v)

  override private[spark] def incCreateClientFactoryTime(v: Long): Unit =
    _createClientFactoryTime.add(v)

  override private[spark] def incCreateClientTime(v: Long): Unit = _createClientTime.add(v)
}
