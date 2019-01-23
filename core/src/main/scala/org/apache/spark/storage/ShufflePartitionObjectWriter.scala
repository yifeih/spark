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

package org.apache.spark.storage

import java.nio.ByteBuffer

import org.apache.spark.serializer.{SerializationStream, SerializerInstance, SerializerManager}
import org.apache.spark.shuffle.ShufflePartitionWriterOutputStream
import org.apache.spark.shuffle.api.{CommittedPartition, ShuffleMapOutputWriter, ShufflePartitionWriter}

/**
 * Replicates the concept of {@link DiskBlockObjectWriter}, but with some key differences:
 * - Naturally, instead of writing to an output file, this writes to the partition writer plugin.
 * - The SerializerManager doesn't wrap the streams for compression and encryption; this work is
 *   left to the implementation of the underlying implementation of the writer plugin.
 */
private[spark] class ShufflePartitionObjectWriter(
    blockId: ShuffleBlockId,
    bufferSize: Int,
    serializerInstance: SerializerInstance,
    serializerManager: SerializerManager,
    mapOutputWriter: ShuffleMapOutputWriter)
  extends PairsWriter {

  // Reused buffer. Experiments should be done with off-heap at some point.
  private val buffer = ByteBuffer.allocate(bufferSize)

  private var currentWriter: ShufflePartitionWriter = _
  private var objectOutputStream: SerializationStream = _

  def startNewPartition(partitionId: Int): Unit = {
    require(buffer.position() == 0,
      "Buffer was not flushed to the underlying output on the previous partition.")
    currentWriter = mapOutputWriter.newPartitionWriter(partitionId)
    val currentWriterStream = new ShufflePartitionWriterOutputStream(
      blockId, currentWriter, buffer, serializerManager)
    objectOutputStream = serializerInstance.serializeStream(currentWriterStream)
  }

  def commitCurrentPartition(): CommittedPartition = {
    require(objectOutputStream != null, "Cannot commit a partition that has not been started.")
    require(currentWriter != null, "Cannot commit a partition that has not been started.")
    objectOutputStream.close()
    val committedPartition = currentWriter.commitPartition()
    buffer.clear()
    currentWriter = null
    objectOutputStream = null
    committedPartition
  }

  def abortCurrentPartition(throwable: Exception): Unit = {
    if (objectOutputStream != null) {
      objectOutputStream.close()
    }

    if (currentWriter != null) {
      currentWriter.abort(throwable)
    }
  }

  def write(key: Any, value: Any): Unit = {
    require(objectOutputStream != null, "Cannot write to a partition that has not been started.")
    objectOutputStream.writeKey(key)
    objectOutputStream.writeValue(value)
  }
}
