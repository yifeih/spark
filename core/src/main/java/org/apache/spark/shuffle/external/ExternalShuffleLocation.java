package org.apache.spark.shuffle.external;

import org.apache.spark.network.protocol.Encoders;
import org.apache.spark.storage.ShuffleLocation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ExternalShuffleLocation implements ShuffleLocation {

    private String shuffleHostname;
    private int shufflePort;

    public ExternalShuffleLocation(String shuffleHostname, int shufflePort) {
        this.shuffleHostname = shuffleHostname;
        this.shufflePort = shufflePort;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(shuffleHostname);
        out.writeInt(shufflePort);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.shuffleHostname = (String) in.readObject();
        this.shufflePort = in.readInt();
    }

    public String getShuffleHostname() {
        return this.shuffleHostname;
    }

    public int getShufflePort() {
        return this.shufflePort;
    }
}
