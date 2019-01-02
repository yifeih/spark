package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.OpenShufflePartition;
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.apache.spark.util.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.Vector;

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
            if (response.hasArray()) {
                // use heap buffer; no array is created; only the reference is used
                return new ByteArrayInputStream(response.array());
            }
            return new ByteBufferInputStream(response);
        } catch (Exception e) {
            logger.error("Encountered exception while trying to fetch blocks", e);
            throw new RuntimeException(e);
        }
    }

    private class StreamCombiningCallback implements StreamCallback {

        public boolean failed;
        private final Vector<InputStream> inputStreams;

        private StreamCombiningCallback() {
            inputStreams = new Vector<>();
            failed = false;
        }

        @Override
        public void onData(String streamId, ByteBuffer buf) throws IOException {
            inputStreams.add(new ByteBufferInputStream(buf));
        }

        @Override
        public void onComplete(String streamId) throws IOException {
            // do nothing
        }

        @Override
        public void onFailure(String streamId, Throwable cause) throws IOException {
            failed = true;
            for (InputStream stream : inputStreams) {
                stream.close();
            }
        }

        private SequenceInputStream getCombinedInputStream() {
            if (failed) {
                throw new RuntimeException("Stream chunk gathering failed");
            }
            return new SequenceInputStream(inputStreams.elements());
        }
    }
}
