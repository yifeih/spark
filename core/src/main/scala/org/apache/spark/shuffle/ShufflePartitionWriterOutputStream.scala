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

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.api.ShufflePartitionWriter
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.{ByteBufferInputStream, Utils}

class ShufflePartitionWriterOutputStream(
    blockId: ShuffleBlockId,
    partitionWriter: ShufflePartitionWriter,
    buffer: ByteBuffer,
    serializerManager: SerializerManager)
  extends OutputStream {

  private var underlyingOutputStream: OutputStream = _

  override def write(b: Int): Unit = {
    buffer.put(b.asInstanceOf[Byte])
    if (buffer.remaining() == 0) {
      pushBufferedBytesToUnderlyingOutput()
    }
  }

  private def pushBufferedBytesToUnderlyingOutput(): Unit = {
    buffer.flip()
    var bufferInputStream: InputStream = new ByteBufferInputStream(buffer)
    if (underlyingOutputStream == null) {
      underlyingOutputStream = serializerManager.wrapStream(blockId,
        partitionWriter.openPartitionStream())
    }
    Utils.copyStream(bufferInputStream, underlyingOutputStream, false, false)
    buffer.clear()
  }

  override def flush(): Unit = {
    pushBufferedBytesToUnderlyingOutput()
    if (underlyingOutputStream != null) {
      underlyingOutputStream.flush();
    }
  }

  override def close(): Unit = {
    flush()
    if (underlyingOutputStream != null) {
      underlyingOutputStream.close()
    }
  }
}
