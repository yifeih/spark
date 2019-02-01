package org.apache.spark.shuffle.external;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.protocol.UploadShufflePartitionStream;
import org.apache.spark.shuffle.api.CommittedPartition;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.storage.ShuffleLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;

public class ExternalShufflePartitionWriter implements ShufflePartitionWriter {

    private static final Logger logger =
        LoggerFactory.getLogger(ExternalShufflePartitionWriter.class);

    private final TransportClientFactory clientFactory;
    private final String hostName;
    private final int port;
    private final String appId;
    private final int shuffleId;
    private final int mapId;
    private final int partitionId;

    private long totalLength = 0;
    private final ByteArrayOutputStream partitionBuffer = new ByteArrayOutputStream();

    public ExternalShufflePartitionWriter(
            TransportClientFactory clientFactory,
            String hostName,
            int port,
            String appId,
            int shuffleId,
            int mapId,
            int partitionId) {
        this.clientFactory = clientFactory;
        this.hostName = hostName;
        this.port = port;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.partitionId = partitionId;
    }

    @Override
    public OutputStream openPartitionStream() { return partitionBuffer; }

    @Override
    public CommittedPartition commitPartition() {
        RpcResponseCallback callback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                logger.info("Successfully uploaded partition");
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("Encountered an error uploading partition", e);
            }
        };
        TransportClient client = null;
        try {
            byte[] buf = partitionBuffer.toByteArray();
            int size = buf.length;
            ByteBuffer streamHeader = new UploadShufflePartitionStream(appId, shuffleId, mapId,
                    partitionId, size).toByteBuffer();
            ManagedBuffer managedBuffer = new NioManagedBuffer(ByteBuffer.wrap(buf));
            client = clientFactory.createUnmanagedClient(hostName, port);
            client.setClientId(String.format("data-%s-%d-%d-%d",
                    appId, shuffleId, mapId, partitionId));
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());
            logger.info("THE BUFFER HASH CODE IS: " + Arrays.hashCode(buf));

            final long startTime = System.nanoTime();
            client.uploadStream(new NioManagedBuffer(streamHeader), managedBuffer, callback);
            final long nanoSeconds = System.nanoTime() - startTime;
            logger.info("METRICS: UploadStream upload time: " + nanoSeconds);
            totalLength += size;
            logger.info("Partition Length: " + totalLength);
            logger.info("Size: " + size);
        } catch (Exception e) {
            if (client != null) {
                client.close();
            }
            logger.error("Encountered error while attempting to upload partition to ESS", e);
            throw new RuntimeException(e);
        } finally {
            logger.info("Successfully sent partition to ESS");
        }
        return new ExternalCommittedPartition(totalLength, new ExternalShuffleLocation(hostName, port));
    }

    @Override
    public void abort(Exception failureReason) {
        try {
            this.partitionBuffer.close();
        } catch(IOException e) {
            logger.error("Failed to close streams after failing to upload partition", e);
        }
        logger.error("Encountered error while attempting" +
            "to upload partition to ESS", failureReason);
    }
}
