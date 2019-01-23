package org.apache.spark.shuffle.external;

import org.apache.spark.storage.ShuffleLocation;

import java.io.*;

public class ExternalShuffleLocation implements ShuffleLocation {

    private String shuffleHostname;
    private int shufflePort;

    public ExternalShuffleLocation() { /* for serialization */ }

    public ExternalShuffleLocation(String shuffleHostname, int shufflePort) {
        this.shuffleHostname = shuffleHostname;
        this.shufflePort = shufflePort;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(shuffleHostname);
        out.writeInt(shufflePort);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        this.shuffleHostname = in.readUTF();
        this.shufflePort = in.readInt();
    }

    public String getShuffleHostname() {
        return this.shuffleHostname;
    }

    public int getShufflePort() {
        return this.shufflePort;
    }
}
