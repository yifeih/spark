package org.apache.spark.shuffle.external;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.OpenShufflePartition;
import org.apache.spark.network.shuffle.protocol.StreamHandle;
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.apache.spark.util.ByteBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.util.Vector;

public class ExternalShufflePartitionReader implements ShufflePartitionReader {

    private static final Logger logger = LoggerFactory.getLogger(ExternalShufflePartitionReader.class);

    private final TransportClient client;
    private final String appId;
    private final int execId;
    private final int shuffleId;
    private final int mapId;

    public ExternalShufflePartitionReader(TransportClient client, String appId, int execId, int shuffleId, int mapId) {
        this.client = client;
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    @Override
    public InputStream fetchPartition(int reduceId) {
        OpenShufflePartition openMessage = new OpenShufflePartition(appId, execId, shuffleId, mapId, reduceId);

        ByteBuffer response = client.sendRpcSync(openMessage.toByteBuffer(), 60000 /* what should be the default?  */);

        try {
            StreamCombiningCallback callback = new StreamCombiningCallback();
            StreamHandle streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteBuffer(response);
            for (int i = 0; i < streamHandle.numChunks; i++) {
                client.stream(OneForOneStreamManager.genStreamChunkId(streamHandle.streamId, i),
                        callback);
            }
            return callback.getCombinedInputStream();
        } catch (Exception e) {
            logger.error("Encountered exception while trying to fetch blocks", e);
            throw new RuntimeException(e);
        }
    }

    private class StreamCombiningCallback implements StreamCallback {

        public boolean failed;
        public final Vector<InputStream> inputStreams;

        public StreamCombiningCallback() {
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

        public SequenceInputStream getCombinedInputStream() {
            if (failed) {
                throw new RuntimeException("Stream chunk gathering failed");
            }
            return new SequenceInputStream(inputStreams.elements());
        }
    }
}
