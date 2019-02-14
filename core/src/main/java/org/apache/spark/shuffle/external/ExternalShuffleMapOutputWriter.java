package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.protocol.RegisterShuffleIndex;
import org.apache.spark.network.shuffle.protocol.UploadShuffleIndex;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


public class ExternalShuffleMapOutputWriter implements ShuffleMapOutputWriter {

    private final TransportClientFactory clientFactory;
    private final String hostName;
    private final int port;
    private final String appId;
    private final int shuffleId;
    private final int mapId;
    private final ShuffleWriteMetricsReporter writeMetrics;
    private final TransportClient client;

    public ExternalShuffleMapOutputWriter(
            TransportClientFactory clientFactory,
            String hostName,
            int port,
            String appId,
            int shuffleId,
            int mapId,
            ShuffleWriteMetricsReporter writeMetrics) {
        this.clientFactory = clientFactory;
        this.hostName = hostName;
        this.port = port;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.writeMetrics = writeMetrics;

        try {
            final long startClientCreationTime = System.nanoTime();
            client = clientFactory.createUnmanagedClient(hostName, port);
            ByteBuffer registerShuffleIndex = new RegisterShuffleIndex(
                    appId, shuffleId, mapId).toByteBuffer();
            String requestID = String.format(
                    "index-register-%s-%d-%d", appId, shuffleId, mapId);
            client.setClientId(requestID);
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());
            final long endClientCreationTime = System.nanoTime();
            writeMetrics.incCreateClientTime(endClientCreationTime - startClientCreationTime);
            client.sendRpcSync(registerShuffleIndex, 60000);
            writeMetrics.incSendRegisterShuffleRequestTime(System.nanoTime() - endClientCreationTime);
        } catch (Exception e) {
            logger.error("Encountered error while creating transport client", e);
            throw new RuntimeException(e);
        }
    }

    private static final Logger logger =
            LoggerFactory.getLogger(ExternalShuffleMapOutputWriter.class);

    @Override
    public ShufflePartitionWriter newPartitionWriter(int partitionId) {
        try {
            return new ExternalShufflePartitionWriter(client,
                hostName, port, appId, shuffleId, mapId, partitionId, writeMetrics);
        } catch (Exception e) {
            clientFactory.close();
            logger.error("Encountered error while creating transport client", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitAllPartitions() {
        try {
            ByteBuffer uploadShuffleIndex = new UploadShuffleIndex(
                    appId, shuffleId, mapId).toByteBuffer();
            String requestID = String.format(
                    "index-upload-%s-%d-%d", appId, shuffleId, mapId);
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());
            final long startTime = System.nanoTime();
            client.sendRpcSync(uploadShuffleIndex, 60000);
            final long nanoSeconds = System.nanoTime() - startTime;
            logger.info("METRICS: UploadIndexParam upload time: " + nanoSeconds);
        } catch (Exception e) {
            logger.error("Encountered error while creating transport client", e);
            client.close();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort(Exception exception) {
        clientFactory.close();
        logger.error("Encountered error while " +
                "attempting to add partitions to ESS", exception);
    }
}
