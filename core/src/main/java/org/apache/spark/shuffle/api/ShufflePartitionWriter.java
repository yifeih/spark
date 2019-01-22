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

import java.io.IOException;
import java.io.OutputStream;

/**
 * Responsible for appending bytes to a partition via passing in input streams incrementally.
 */
public interface ShufflePartitionWriter {

  /**
   * Return a stream that should persist the bytes for this partition.
   */
  OutputStream openPartitionStream() throws IOException;

  /**
   * Indicate that the partition was written successfully and there are no more incoming bytes.
   * Returns a {@link CommittedPartition} indicating information about that written partition.
   */
  CommittedPartition commitPartition() throws IOException;

  /**
   * Indicate that the write has failed for some reason and the implementation can handle the
   * failure reason. After this method is called, this writer will be discarded; it's expected that
   * the implementation will close any underlying resources.
   */
  void abort(Exception failureReason) throws IOException;
}
