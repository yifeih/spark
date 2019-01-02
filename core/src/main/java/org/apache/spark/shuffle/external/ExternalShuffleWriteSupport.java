package org.apache.spark.shuffle.external;

import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.crypto.AuthClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.shuffle.protocol.UploadShuffleIndexStream;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.List;

public class ExternalShuffleWriteSupport implements ShuffleWriteSupport {

    private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleWriteSupport.class);

    private final TransportConf conf;
    private final boolean authEnabled;
    private final SecretKeyHolder secretKeyHolder;
    private final String hostname;
    private final int port;
    private final String execId;

    public ExternalShuffleWriteSupport(
            TransportConf conf, boolean authEnabled, SecretKeyHolder secretKeyHolder,
            String hostname, int port, String execId) {
        this.conf = conf;
        this.authEnabled = authEnabled;
        this.secretKeyHolder = secretKeyHolder;
        this.hostname = hostname;
        this.port = port;
        this.execId = execId;
    }

    @Override
    public ShuffleMapOutputWriter newMapOutputWriter(String appId, int shuffleId, int mapId) {
        TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        if (authEnabled) {
            bootstraps.add(new AuthClientBootstrap(conf, appId, secretKeyHolder));
        }
        TransportClientFactory clientFactory = context.createClientFactory(bootstraps);
        return new ShuffleMapOutputWriter() {
            @Override
            public ShufflePartitionWriter newPartitionWriter(int partitionId) {
                try {
                    TransportClient client = clientFactory.createClient(hostname, port);
                    return new ExternalShufflePartitionWriter(
                            client, appId, execId, shuffleId, mapId, partitionId);
                } catch (Exception e) {
                    logger.error("Encountered error while creating transport client", e);
                    throw new RuntimeException(e); // what is standard practice here?
                }
            }

            @Override
            public void commitAllPartitions(long[] partitionLengths) {
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
                final TransportClient client;
                try {
                    client = clientFactory.createClient(hostname, port);
                    logger.info("Committing all partitions with a creation of an index file");
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
                    client.close();
                } catch (Exception e) {
                    // Close client upon failure
                    logger.error("Encountered error while creating transport client", e);
                }
            }

            @Override
            public void abort(Exception exception) {
                logger.error("Encountered error while" +
                    "attempting to all partitions to ESS", exception);
            }
        };
    }
}
