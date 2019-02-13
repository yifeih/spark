package org.apache.spark.shuffle.external;

import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.crypto.AuthClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExternalShuffleWriteSupport implements ShuffleWriteSupport {

  private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleWriteSupport.class);

  private final TransportConf conf;
  private final TransportContext context;
  private final boolean authEnabled;
  private final SecretKeyHolder secretKeyHolder;
  private final String hostname;
  private final int port;

  public ExternalShuffleWriteSupport(
    TransportConf conf,
    TransportContext context,
    boolean authEnabled,
    SecretKeyHolder secretKeyHolder,
    String hostname,
    int port) {
  this.conf = conf;
  this.context = context;
  this.authEnabled = authEnabled;
  this.secretKeyHolder = secretKeyHolder;
  this.hostname = hostname;
  this.port = port;
}

  @Override
  public ShuffleMapOutputWriter newMapOutputWriter(String appId, int shuffleId, int mapId,
                                                   ShuffleWriteMetricsReporter writeMetrics) {
    final long startClientFactoryCreationTime = System.nanoTime();
    List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
    if (authEnabled) {
      bootstraps.add(new AuthClientBootstrap(conf, appId, secretKeyHolder));
    }
    TransportClientFactory clientFactory = context.createClientFactory(bootstraps);
    logger.info("Clientfactory: " + clientFactory.toString());
    writeMetrics.incCreateClientFactoryTime(System.nanoTime() - startClientFactoryCreationTime);
    return new ExternalShuffleMapOutputWriter(
      clientFactory, hostname, port, appId, shuffleId, mapId, writeMetrics);
  }
}
