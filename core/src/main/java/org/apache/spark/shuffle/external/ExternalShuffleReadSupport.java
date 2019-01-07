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
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExternalShuffleReadSupport implements ShuffleReadSupport {

    private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleReadSupport.class);

    private final TransportConf conf;
    private final boolean authEnabled;
    private final SecretKeyHolder secretKeyHolder;
    private final String hostname;
    private final int port;
    private final String execId;

    public ExternalShuffleReadSupport(
            TransportConf conf,
            boolean authEnabled,
            SecretKeyHolder secretKeyHolder,
            String hostname,
            int port,
            String execId) {
        this.conf = conf;
        this.authEnabled = authEnabled;
        this.secretKeyHolder = secretKeyHolder;
        this.hostname = hostname;
        this.port = port;
        this.execId = execId;
    }

    @Override
    public ShufflePartitionReader newPartitionReader(String appId, int shuffleId, int mapId) {
        // TODO combine this into a function with ExternalShuffleWriteSupport
        TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), true, true);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        if (authEnabled) {
            bootstraps.add(new AuthClientBootstrap(conf, appId, secretKeyHolder));
        }
        TransportClientFactory clientFactory = context.createClientFactory(bootstraps);
        try {
            TransportClient client = clientFactory.createClient(hostname, port);
            return new ExternalShufflePartitionReader(client, appId, execId, shuffleId, mapId);
        } catch (Exception e) {
            clientFactory.close();
            logger.error("Encountered creating transport client for partition reader");
            throw new RuntimeException(e); // what is standard practice here?
        }
    }
}
