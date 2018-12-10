package org.apache.spark.shuffle.external;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.UploadShufflePartitionStream;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ExternalShufflePartitionWriter implements ShufflePartitionWriter {

    private static final Logger logger = LoggerFactory.getLogger(ExternalShufflePartitionWriter.class);

    private final TransportClient client;
    private final String appId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShufflePartitionWriter(TransportClient client, String appId, int shuffleId, int mapId) {
        this.client = client;
        this.appId = appId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    @Override
    public int appendPartition(int partitionId, InputStream partitionInputStream) {
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
        try {
            ByteBuffer streamHeader =
                    new UploadShufflePartitionStream(this.appId, shuffleId, mapId).toByteBuffer();
            int avaibleSize = partitionInputStream.available();
            byte[] buf = new byte[avaibleSize];
            int size = partitionInputStream.read(buf, 0, avaibleSize);
            assert size == avaibleSize;
            ManagedBuffer managedBuffer = new NioManagedBuffer(ByteBuffer.wrap(buf));
            client.uploadStream(new NioManagedBuffer(streamHeader), managedBuffer, callback);
            return size;
        } catch (Exception e) {
            logger.error("Encountered error while attempting to upload partition to ESS", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort() {
        // should i abort everything i've written with this writer?
    }

    @Override
    public void close() throws IOException {
        // how to commit?
    }
}
