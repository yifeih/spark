package org.apache.spark.network.shuffle.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

public class OpenShufflePartitionStream extends BlockTransferMessage {
    public final String appId;
    public final int execId;
    public final int shuffleId;
    public final int mapId;
    public final int reduceId;

    public OpenShufflePartitionStream(String appId, int execId, int shuffleId, int mapId, int reduceId) {
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.reduceId = reduceId;
    }

    @Override
    protected Type type() {
        return null;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) + 4 + 4 + 4 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {

    }

    public static OpenShufflePartitionStream decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        int execId = buf.readInt();
        int shuffleId = buf.readInt();

    }
}
