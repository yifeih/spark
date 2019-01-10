package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalShuffleMapOutputWriter implements ShuffleMapOutputWriter {

    private final TransportClientFactory clientFactory;
    private final String hostName;
    private final int port;
    private final String appId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShuffleMapOutputWriter(
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

    private static final Logger logger =
            LoggerFactory.getLogger(ExternalShuffleMapOutputWriter.class);

    @Override
    public ShufflePartitionWriter newPartitionWriter(int partitionId) {
        try {
            return new ExternalShufflePartitionWriter(clientFactory,
                hostName, port, appId, shuffleId, mapId, partitionId);
        } catch (Exception e) {
            clientFactory.close();
            logger.error("Encountered error while creating transport client", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitAllPartitions(long[] partitionLengths) {
        try {
            ExternalShuffleIndexWriter externalShuffleIndexWriter =
                new ExternalShuffleIndexWriter(clientFactory,
                    hostName, port, appId, shuffleId, mapId);
            externalShuffleIndexWriter.write(partitionLengths);
        } catch (Exception e) {
            clientFactory.close();
            logger.error("Encountered error writing index file", e);
            throw new RuntimeException(e); // what is standard practice here?
        }
    }

    @Override
    public void abort(Exception exception) {
        clientFactory.close();
        logger.error("Encountered error while " +
                "attempting to add partitions to ESS", exception);
    }
}
