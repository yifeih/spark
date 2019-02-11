package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.protocol.OpenShufflePartition;
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.apache.spark.storage.ShuffleLocation;
import org.apache.spark.util.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

public class ExternalShufflePartitionReader implements ShufflePartitionReader {

    private static final Logger logger =
        LoggerFactory.getLogger(ExternalShufflePartitionReader.class);

    private final TransportClientFactory clientFactory;
    private final String appId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShufflePartitionReader(
            TransportClientFactory clientFactory,
            String appId,
            int shuffleId,
            int mapId) {
        this.clientFactory = clientFactory;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    @Override
    public InputStream fetchPartition(int reduceId, Optional<ShuffleLocation> shuffleLocation) {
        assert shuffleLocation.isPresent() &&
                shuffleLocation.get() instanceof ExternalShuffleLocation;
        ExternalShuffleLocation externalShuffleLocation =
                (ExternalShuffleLocation) shuffleLocation.get();
        logger.info(String.format("Found external shuffle location on node: %s:%d",
                externalShuffleLocation.getShuffleHostname(),
                externalShuffleLocation.getShufflePort()));
        String hostname = externalShuffleLocation.getShuffleHostname();
        int port = externalShuffleLocation.getShufflePort();

        OpenShufflePartition openMessage =
            new OpenShufflePartition(appId, shuffleId, mapId, reduceId);
        TransportClient client = null;
        try {
            client = clientFactory.createUnmanagedClient(hostname, port);
            String requestID = String.format(
                    "read-%s-%d-%d-%d", appId, shuffleId, mapId, reduceId);
            client.setClientId(requestID);
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());

            ByteBuffer response = client.sendRpcSync(openMessage.toByteBuffer(), 60000);
            logger.info("response is: " + response.toString() +
                " " + response.array() + " " + response.hasArray());
            if (response.hasArray()) {
                logger.info("response hashcode: " + Arrays.hashCode(response.array()));
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
        }
    }
}
