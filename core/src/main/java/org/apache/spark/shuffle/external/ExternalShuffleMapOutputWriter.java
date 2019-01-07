package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalShuffleMapOutputWriter implements ShuffleMapOutputWriter {

    private final TransportClientFactory clientFactory;
    private final String hostname;
    private final int port;
    private final String appId;
    private final String execId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShuffleMapOutputWriter(
            TransportClientFactory clientFactory,
            String hostname,
            int port,
            String appId,
            String execId,
            int shuffleId,
            int mapId) {
        this.clientFactory = clientFactory;
        this.hostname = hostname;
        this.port = port;
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    private static final Logger logger =
            LoggerFactory.getLogger(ExternalShuffleMapOutputWriter.class);

    @Override
    public ShufflePartitionWriter newPartitionWriter(int partitionId) {
        try {
            TransportClient client = clientFactory.createUnmanagedClient(hostname, port);
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());
            return new ExternalShufflePartitionWriter(
                    client, appId, execId, shuffleId, mapId, partitionId);
        } catch (Exception e) {
            clientFactory.close();
            logger.error("Encountered error while creating transport client", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitAllPartitions(long[] partitionLengths) {
        try {
            TransportClient client = clientFactory.createUnmanagedClient(hostname, port);
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());
            ExternalShuffleIndexWriter externalShuffleIndexWriter =
                new ExternalShuffleIndexWriter(
                    client, appId, execId, shuffleId, mapId);
            externalShuffleIndexWriter.write(partitionLengths);
        } catch (Exception e) {
            clientFactory.close();
            logger.error("Encountered error while creating transport client", e);
            throw new RuntimeException(e); // what is standard practice here?
        }
    }

    @Override
    public void abort(Exception exception) {
        clientFactory.close();
        logger.error("Encountered error while" +
                "attempting to add partitions to ESS", exception);
    }
}
