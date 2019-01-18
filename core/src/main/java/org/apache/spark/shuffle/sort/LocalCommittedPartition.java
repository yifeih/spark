package org.apache.spark.shuffle.sort;

import org.apache.spark.shuffle.api.CommittedPartition;
import org.apache.spark.storage.ShuffleLocation;

import java.util.Optional;

public class LocalCommittedPartition implements CommittedPartition {

    private final long length;

    public LocalCommittedPartition(long length) {
        this.length = length;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public Optional<ShuffleLocation> shuffleLocation() {
        return Optional.empty();
    }
}
