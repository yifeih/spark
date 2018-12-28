package org.apache.spark.shuffle.external;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.network.netty.SparkTransportConf;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.apache.spark.SecurityManager;
import org.apache.spark.util.Utils;

public class ExternalShuffleDataIO implements ShuffleDataIO {

    private static final String SHUFFLE_SERVICE_PORT_CONFIG = "spark.shuffle.service.port";
    private static final String DEFAULT_SHUFFLE_PORT = "7337";

    private static final SparkEnv sparkEnv = SparkEnv.get();

    private final SparkConf sparkConf;
    private final TransportConf conf;
    private final SecurityManager securityManager;
    private final String hostname;
    private final int port;
    private final String execId;

    public ExternalShuffleDataIO(
            SparkConf sparkConf) {
        this.sparkConf = sparkConf;
        this.conf = SparkTransportConf.fromSparkConf(sparkConf, "shuffle", 2);

        this.securityManager = sparkEnv.securityManager();
        this.hostname = sparkEnv.blockManager().blockTransferService().hostName();

        int tmpPort = Integer.parseInt(Utils.getSparkOrYarnConfig(
            sparkConf, SHUFFLE_SERVICE_PORT_CONFIG, DEFAULT_SHUFFLE_PORT));
        if (tmpPort == 0) {
            this.port = Integer.parseInt(sparkConf.get(SHUFFLE_SERVICE_PORT_CONFIG));
        } else {
            this.port = tmpPort;
        }

        this.execId = SparkEnv.get().blockManager().shuffleServerId().executorId();
    }

    @Override
    public void initialize() {
        // TODO: move registerDriver and registerExecutor here
    }

    @Override
    public ShuffleReadSupport readSupport() {
        return new ExternalShuffleReadSupport(
                conf, securityManager.isAuthenticationEnabled(),
                securityManager, hostname, port, execId);
    }

    @Override
    public ShuffleWriteSupport writeSupport() {
        return new ExternalShuffleWriteSupport(
                conf, securityManager.isAuthenticationEnabled(),
                securityManager, hostname, port, execId);
    }
}
