package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.OpenShufflePartition;
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.apache.spark.util.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ExternalShufflePartitionReader implements ShufflePartitionReader {

    private static final Logger logger =
        LoggerFactory.getLogger(ExternalShufflePartitionReader.class);

    private final TransportClient client;
    private final String appId;
    private final String execId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShufflePartitionReader(
            TransportClient client,
            String appId,
            String execId,
            int shuffleId,
            int mapId) {
        this.client = client;
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    @Override
    public InputStream fetchPartition(int reduceId) {
        OpenShufflePartition openMessage =
            new OpenShufflePartition(appId, execId, shuffleId, mapId, reduceId);
        ByteBuffer response = client.sendRpcSync(openMessage.toByteBuffer(), 60000);
        try {
//            logger.info("response is: " + response.toString() + " " + response.getDouble());
            if (response.hasArray()) {
                // use heap buffer; no array is created; only the reference is used
                return new ByteArrayInputStream(response.array());
            }
            return new ByteBufferInputStream(response);
        } catch (Exception e) {
            this.client.close();
            logger.error("Encountered exception while trying to fetch blocks", e);
            throw new RuntimeException(e);
        } finally {
            this.client.close();
        }
    }
}
