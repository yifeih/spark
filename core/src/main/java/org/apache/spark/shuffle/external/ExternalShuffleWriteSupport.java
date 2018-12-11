package org.apache.spark.shuffle.external;

import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.crypto.AuthClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            TransportConf conf, boolean authEnabled, SecretKeyHolder secretKeyHolder, String hostname, int port, String execId) {
        this.conf = conf;
        this.authEnabled = authEnabled;
        this.secretKeyHolder = secretKeyHolder;
        this.hostname = hostname;
        this.port = port;
        this.execId = execId;
    }

    @Override
    public ShufflePartitionWriter newPartitionWriter(String appId, int shuffleId, int mapId, int partitionId) {
        TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        if (authEnabled) {
            bootstraps.add(new AuthClientBootstrap(conf, appId, secretKeyHolder));
        }
        TransportClientFactory clientFactory = context.createClientFactory(bootstraps);
        try {
            TransportClient client = clientFactory.createClient(hostname, port);
            return new ExternalShufflePartitionWriter(client, appId, execId, shuffleId, mapId, partitionId);
        } catch (Exception e) {
            logger.error("Encountered error while creating transport client");
            throw new RuntimeException(e); // what is standard practice here?
        }
    }
}
