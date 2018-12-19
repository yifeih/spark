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

package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

/**
 * Upload shuffle partition request to the External Shuffle Service.
 * This request should also include the driverHostPort for the sake of
 * setting up a driver heartbeat to monitor heartbeat
 */
public class UploadShufflePartitionStream extends BlockTransferMessage {
    public final String driverHostPort;
    public final String appId;
    public final String execId;
    public final int shuffleId;
    public final int mapId;
    public final int partitionId;

    public UploadShufflePartitionStream(
            String driverHostPort,
            String appId,
            String execId,
            int shuffleId,
            int mapId,
            int partitionId) {
        this.driverHostPort = driverHostPort;
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof UploadShufflePartitionStream) {
            UploadShufflePartitionStream o = (UploadShufflePartitionStream) other;
            return Objects.equal(appId, o.appId)
                    && driverHostPort == o.driverHostPort
                    && execId == o.execId
                    && shuffleId == o.shuffleId
                    && mapId == o.mapId
                    && partitionId == o.partitionId;
        }
        return false;
    }

    @Override
    protected Type type() {
        return Type.UPLOAD_SHUFFLE_PARTITION_STREAM;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(driverHostPort, appId, execId, shuffleId, mapId, partitionId);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("driverHostPort", driverHostPort)
                .add("appId", appId)
                .add("execId", execId)
                .add("shuffleId", shuffleId)
                .add("mapId", mapId)
                .toString();
    }

    @Override
    public int encodedLength() {
        return  Encoders.Strings.encodedLength(driverHostPort) + Encoders.Strings.encodedLength(appId) +
                Encoders.Strings.encodedLength(execId) + 4 + 4 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, driverHostPort);
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        buf.writeInt(shuffleId);
        buf.writeInt(mapId);
        buf.writeInt(partitionId);
    }

    public static UploadShufflePartitionStream decode(ByteBuf buf) {
        String driverHostPort = Encoders.Strings.decode(buf);
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        int shuffleId = buf.readInt();
        int mapId = buf.readInt();
        int partitionId = buf.readInt();
        return new UploadShufflePartitionStream(driverHostPort, appId, execId, shuffleId, mapId, partitionId);
    }
}
