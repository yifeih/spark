package org.apache.spark.shuffle.external;

import org.apache.spark.shuffle.api.ShufflePartitionReader;

import java.io.InputStream;

public class ExternalShufflePartitionReader implements ShufflePartitionReader {
    @Override
    public InputStream fetchPartition(int reduceId) {
        return null;
    }
}
