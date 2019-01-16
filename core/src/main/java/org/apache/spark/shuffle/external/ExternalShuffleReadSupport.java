package org.apache.spark.shuffle.external;

import com.google.common.collect.Lists;
import org.apache.spark.MapOutputTracker;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.crypto.AuthClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.apache.spark.storage.ShuffleLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.compat.java8.OptionConverters;

import java.util.List;
import java.util.Optional;

public class ExternalShuffleReadSupport implements ShuffleReadSupport {

    private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleReadSupport.class);

    private final TransportConf conf;
    private final boolean authEnabled;
    private final SecretKeyHolder secretKeyHolder;
    private final MapOutputTracker mapOutputTracker;

    public ExternalShuffleReadSupport(
            TransportConf conf,
            boolean authEnabled,
            SecretKeyHolder secretKeyHolder,
            MapOutputTracker mapOutputTracker) {
        this.conf = conf;
        this.authEnabled = authEnabled;
        this.secretKeyHolder = secretKeyHolder;
        this.mapOutputTracker = mapOutputTracker;
    }

    @Override
    public ShufflePartitionReader newPartitionReader(String appId, int shuffleId, int mapId) {
        // TODO combine this into a function with ExternalShuffleWriteSupport
        TransportContext context = new TransportContext(conf, new NoOpRpcHandler(), false);
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        if (authEnabled) {
            bootstraps.add(new AuthClientBootstrap(conf, appId, secretKeyHolder));
        }
        TransportClientFactory clientFactory = context.createClientFactory(bootstraps);
        Optional<ShuffleLocation> maybeShuffleLocation = OptionConverters.toJava(mapOutputTracker.getShuffleLocation(shuffleId, mapId));
        assert maybeShuffleLocation.isPresent();
        ExternalShuffleLocation externalShuffleLocation = (ExternalShuffleLocation) maybeShuffleLocation.get();
        logger.info(String.format("Found external shuffle location on node: %s:%d",
                externalShuffleLocation.getShuffleHostname(),
                externalShuffleLocation.getShufflePort()));
        try {
            return new ExternalShufflePartitionReader(clientFactory,
                externalShuffleLocation.getShuffleHostname(),
                    externalShuffleLocation.getShufflePort(),
                    appId,
                    shuffleId,
                    mapId);
        } catch (Exception e) {
            clientFactory.close();
            logger.error("Encountered creating transport client for partition reader");
            throw new RuntimeException(e); // what is standard practice here?
        }
    }
}
