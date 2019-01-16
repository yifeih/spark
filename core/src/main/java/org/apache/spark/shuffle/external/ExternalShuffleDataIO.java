package org.apache.spark.shuffle.external;

import org.apache.spark.MapOutputTracker;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.netty.SparkTransportConf;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.apache.spark.SecurityManager;
import org.apache.spark.storage.BlockManager;

public class ExternalShuffleDataIO implements ShuffleDataIO {

    private final TransportConf conf;
    private final TransportContext context;
    private static BlockManager blockManager;
    private static SecurityManager securityManager;
    private static String hostname;
    private static int port;
    private static MapOutputTracker mapOutputTracker;

    public ExternalShuffleDataIO(
            SparkConf sparkConf) {
        this.conf = SparkTransportConf.fromSparkConf(sparkConf, "shuffle", 1);
        // Close idle connections
        this.context = new TransportContext(conf, new NoOpRpcHandler(), true, true);
    }

    @Override
    public void initialize() {
        SparkEnv env = SparkEnv.get();
        blockManager = env.blockManager();
        securityManager = env.securityManager();
        hostname = blockManager.getRandomShuffleHost();
        port = blockManager.getRandomShufflePort();
        mapOutputTracker = env.mapOutputTracker();
        // TODO: Register Driver and Executor
    }

    @Override
    public ShuffleReadSupport readSupport() {
        return new ExternalShuffleReadSupport(
                conf, context, securityManager.isAuthenticationEnabled(),
                securityManager, mapOutputTracker);
    }

    @Override
    public ShuffleWriteSupport writeSupport() {
        return new ExternalShuffleWriteSupport(
            conf, context, securityManager.isAuthenticationEnabled(),
            securityManager, hostname, port);
    }
}
