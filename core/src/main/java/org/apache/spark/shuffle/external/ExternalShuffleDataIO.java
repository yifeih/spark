package org.apache.spark.shuffle.external;

import org.apache.spark.SparkConf;
import org.apache.spark.network.netty.SparkTransportConf;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.apache.spark.SecurityManager;

public class ExternalShuffleDataIO implements ShuffleDataIO {

    private final SparkConf sparkConf;
    private final TransportConf conf;
    private final SecurityManager securityManager;
    private final String hostname;
    private final int port;
    private final int execId;

    public ExternalShuffleDataIO(
            SparkConf sparkConf,
            SecurityManager securityManager,
            String hostname, // start out with one, TODO: make this compatible with multiple
            int port,
            int execId) {
        this.sparkConf = sparkConf;
        this.conf = SparkTransportConf.fromSparkConf(sparkConf, "shuffle", 0 /* ? */);
        this.securityManager = securityManager;
        this.hostname = hostname;
        this.port = port;
        this.execId = execId;
    }

    @Override
    public void initialize() {
        // figure out initialization
    }

    @Override
    public ShuffleReadSupport readSupport() {
        return new ExternalShuffleReadSupport(
                conf, securityManager.isAuthenticationEnabled(), securityManager, hostname, port, execId);
    }

    @Override
    public ShuffleWriteSupport writeSupport() {
        return new ExternalShuffleWriteSupport(
                conf, securityManager.isAuthenticationEnabled(), securityManager, hostname, port, execId);
    }
}
