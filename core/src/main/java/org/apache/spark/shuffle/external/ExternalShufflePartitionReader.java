package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
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

    private final TransportClientFactory clientFactory;
    private final String hostName;
    private final int port;
    private final String appId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShufflePartitionReader(
            TransportClientFactory clientFactory,
            String hostName,
            int port,
            String appId,
            int shuffleId,
            int mapId) {
        this.clientFactory = clientFactory;
        this.hostName = hostName;
        this.port = port;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    @Override
    public InputStream fetchPartition(int reduceId) {
        OpenShufflePartition openMessage =
            new OpenShufflePartition(appId, shuffleId, mapId, reduceId);
        TransportClient client = null;
        try {
            client = clientFactory.createUnmanagedClient(hostName, port);
            client.setClientId(String.format(
                "read-%s-%d-%d-%d", appId, shuffleId, mapId, reduceId));
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());
            ByteBuffer response = client.sendRpcSync(openMessage.toByteBuffer(), 60000);
            logger.info("response is: " + response.toString() +
                " " + response.array() + " " + response.hasArray());
            if (response.hasArray()) {
                // use heap buffer; no array is created; only the reference is used
                return new ByteArrayInputStream(response.array());
            }
            return new ByteBufferInputStream(response);
        } catch (Exception e) {
            if (client != null) {
                client.close();
            }
            logger.error("Encountered exception while trying to fetch blocks", e);
            throw new RuntimeException(e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
