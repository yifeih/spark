package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

public class OpenShufflePartition extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final int shuffleId;
    public final int mapId;
    public final int partitionId;

    public OpenShufflePartition(String appId, String execId, int shuffleId, int mapId, int partitionId) {
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof OpenShufflePartition) {
            OpenShufflePartition o = (OpenShufflePartition) other;
            return Objects.equal(appId, o.appId)
                    && execId == o.execId
                    && shuffleId == o.shuffleId
                    && mapId == o.mapId
                    && partitionId == o.partitionId;
        }
        return false;
    }

    @Override
    protected Type type() {
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId, shuffleId, mapId, partitionId);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("shuffleId", shuffleId)
                .add("mapId", mapId)
                .toString();
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) + Encoders.Strings.encodedLength(execId) + 4 + 4 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        buf.writeInt(shuffleId);
        buf.writeInt(mapId);
        buf.writeInt(partitionId);
    }

    public static OpenShufflePartition decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        int shuffleId = buf.readInt();
        int mapId = buf.readInt();
        int partitionId = buf.readInt();
        return new OpenShufflePartition(appId, execId, shuffleId, mapId, partitionId);
    }
}
