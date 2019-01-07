package org.apache.spark.shuffle.external;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.UploadShuffleIndexStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class ExternalShuffleIndexWriter {

    private final TransportClient client;
    private final String appId;
    private final String execId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShuffleIndexWriter(
            TransportClient client,
            String appId,
            String execId,
            int shuffleId,
            int mapId){
        this.client = client;
        this.appId = appId;
        this.execId = execId;
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
        try {
            logger.info("clientid: " + client.getClientId() + " " + client.isActive());
            logger.info("Committing all partitions with a creation of an index file");
            logger.info("Partition Lengths: " + partitionLengths[0] + ": " + partitionLengths.length);
            ByteBuffer streamHeader = new UploadShuffleIndexStream(
                appId, execId, shuffleId, mapId).toByteBuffer();
            // Size includes first 0L offset
            ByteBuffer byteBuffer = ByteBuffer.allocate(8 + (partitionLengths.length * 8));
            LongBuffer longBuffer = byteBuffer.asLongBuffer();
            Long offset = 0L;
            longBuffer.put(offset);
            for (Long length: partitionLengths) {
                offset += length;
                longBuffer.put(offset);
            }
            client.uploadStream(new NioManagedBuffer(streamHeader),
                    new NioManagedBuffer(byteBuffer), callback);
        } catch (Exception e) {
            client.close();
            logger.error("Encountered error while creating transport client", e);
        }
    }
}
