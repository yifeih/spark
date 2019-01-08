package org.apache.spark.shuffle.external;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.shuffle.protocol.UploadShuffleIndexStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class ExternalShuffleIndexWriter {

    private final TransportClientFactory clientFactory;
    private final String hostName;
    private final int port;
    private final String appId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShuffleIndexWriter(
            TransportClientFactory clientFactory,
            String hostName,
            int port,
            String appId,
            int shuffleId,
            int mapId){
        this.clientFactory = clientFactory;
        this.hostName = hostName;
        this.port = port;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    private static final Logger logger =
        LoggerFactory.getLogger(ExternalShuffleIndexWriter.class);

    public void write(long[] partitionLengths) {
        RpcResponseCallback callback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                logger.info("Successfully uploaded index");
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("Encountered an error uploading index", e);
            }
        };
        TransportClient client = null;
        try {
            logger.info("Committing all partitions with a creation of an index file");
            logger.info("Partition Lengths: " + partitionLengths.length + ": "
                    + partitionLengths[0] + "," + partitionLengths[1]);
            ByteBuffer streamHeader = new UploadShuffleIndexStream(
                appId, shuffleId, mapId).toByteBuffer();
            // Size includes first 0L offset
            ByteBuffer byteBuffer = ByteBuffer.allocate(8 + (partitionLengths.length * 8));
            LongBuffer longBuffer = byteBuffer.asLongBuffer();
            Long offset = 0L;
            longBuffer.put(offset);
            for (Long length: partitionLengths) {
                offset += length;
                longBuffer.put(offset);
            }
            client = clientFactory.createUnmanagedClient(hostName, port);
            client.setClientId(String.format("index-%s-%d-%d", appId, shuffleId, mapId));
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());
            client.uploadStream(new NioManagedBuffer(streamHeader),
                    new NioManagedBuffer(byteBuffer), callback);
        } catch (Exception e) {
            client.close();
            logger.error("Encountered error while creating transport client", e);
        }
    }
}
