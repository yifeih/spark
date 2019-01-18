package org.apache.spark.shuffle.api;

import org.apache.spark.storage.ShuffleLocation;

import java.util.Optional;

public interface CommittedPartition {

    /**
     * Indicates the number of bytes written in a committed partition.
     * Note that returning the length is mainly for backwards compatibility
     * and should be removed in a more polished variant. After this method
     * is called, the writer will be discarded; it's expected that the
     * implementation will close any underlying resources.
     */
    long length();

    /**
     * Indicates the shuffle location to which this partition was written.
     * Some implementations may not need to specify a shuffle location.
     */
    Optional<ShuffleLocation> shuffleLocation();
}
