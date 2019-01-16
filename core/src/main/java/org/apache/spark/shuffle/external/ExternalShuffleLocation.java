package org.apache.spark.shuffle.external;

import org.apache.hadoop.mapreduce.task.reduce.Shuffle;
import org.apache.spark.network.protocol.Encoders;
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
//        out.writeInt(shuffleHostname.length());
//        out.writeChars(shuffleHostname);
        out.writeUTF(shuffleHostname);
        out.writeInt(shufflePort);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//        int size = in.readInt();
//        byte[] buf = new byte[size];
//        in.read(buf, 0, size);
//        this.shuffleHostname = new String(buf);
        this.shuffleHostname = in.readUTF();
        this.shufflePort = in.readInt();
    }

    public String getShuffleHostname() {
        return this.shuffleHostname;
    }

    public int getShufflePort() {
        return this.shufflePort;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ExternalShuffleLocation externalShuffleLocation = new ExternalShuffleLocation("hostname", 1234);
        ShuffleLocation shuffleLocation = (ShuffleLocation) externalShuffleLocation;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(shuffleLocation);
        oos.flush();


        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        ShuffleLocation newShuffLocation = (ShuffleLocation) ois.readObject();
        System.out.println(newShuffLocation);
    }
}
