package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.shuffle.api.ShufflePartitionReader;

import java.io.InputStream;

public class ExternalShufflePartitionReader implements ShufflePartitionReader {

    private final TransportClient client;
    private final String appId;
    private final int execId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShufflePartitionReader(TransportClient client, String appId, int execId, int shuffleId, int mapId) {
        this.client = client;
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    @Override
    public InputStream fetchPartition(int reduceId) {
        return null;
    }
}
