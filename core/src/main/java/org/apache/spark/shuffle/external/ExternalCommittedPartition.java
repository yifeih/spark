package org.apache.spark.shuffle.external;

import org.apache.spark.shuffle.api.CommittedPartition;
import org.apache.spark.storage.ShuffleLocation;

import java.util.Optional;

public class ExternalCommittedPartition implements CommittedPartition {

    private final long length;
    private final Optional<ShuffleLocation> shuffleLocation;

    public ExternalCommittedPartition(long length) {
        this.length = length;
        this.shuffleLocation = Optional.empty();
    }

    public ExternalCommittedPartition(long length, ShuffleLocation shuffleLocation) {
        this.length = length;
        this.shuffleLocation = Optional.of(shuffleLocation);
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public Optional<ShuffleLocation> shuffleLocation() {
        return shuffleLocation;
    }
}
