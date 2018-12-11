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

package org.apache.spark.shuffle.api;

import java.io.InputStream;

/**
 * Responsible for appending bytes to a partition via passing in input streams incrementally.
 */
public interface ShufflePartitionWriter {

  /**
   * Another option is to have this take no parameters and return an OutputStream. But we choose
   * to pass an InputStream directly because the writer may have different opinions about how
   * to forward the data to the backing system. As an example, the writer can choose to take each
   * byte from the incoming input stream and multicast ot to multiple backends for replication.
   * So OutputStream is too restrictive of an API.
   */
  void appendBytesToPartition(InputStream streamReadingBytesToAppend);

  /**
   * Indicate that the partition was written successfully and there are no more incoming bytes. Returns
   * the length of the partition that is written. Note that returning the length is mainly for backwards
   * compatibility and should be removed in a more polished variant. After this method is called, the writer
   * will be discarded; it's expected that the implementation will close any underlying resources.
   */
  long commitAndGetTotalLength();

  /**
   * Indicate that the write has failed for some reason and the implementation can handle the
   * failure reason. After this method is called, this writer will be discarded; it's expected that
   * the implementation will close any underlying resources.
   */
  void abort(Exception failureReason);
}
