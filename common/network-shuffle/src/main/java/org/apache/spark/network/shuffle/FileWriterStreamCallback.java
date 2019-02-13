package org.apache.spark.network.shuffle;

import org.apache.spark.network.client.RpcResponseCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.apache.spark.network.client.StreamCallbackWithID;

public class FileWriterStreamCallback implements StreamCallbackWithID {

  private static final Logger logger = LoggerFactory.getLogger(FileWriterStreamCallback.class);

  public enum FileType {
    DATA("shuffle-data"),
    INDEX("shuffle-index");

    private final String typeString;

      FileType(String typeString) {
      this.typeString = typeString;
    }

    @Override
    public String toString() {
      return typeString;
    }
  }

  private final String appId;
  private final int shuffleId;
  private final int mapId;
  private final File file;
  private final FileType fileType;
  private WritableByteChannel fileOutputChannel = null;
  private long startTime;
  private final RpcResponseCallback callback;

  public FileWriterStreamCallback(
    String appId,
    int shuffleId,
    int mapId,
    File file,
    FileWriterStreamCallback.FileType fileType,
    RpcResponseCallback callback) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.file = file;
    this.fileType = fileType;
    this.startTime = 0;
    this.callback = callback;
  }

  public void open() {
    this.startTime = System.nanoTime();
    logger.info(
        "Opening {} for remote writing. File type: {}", file.getAbsolutePath(), fileType);
    if (fileOutputChannel != null) {
      throw new IllegalStateException(
          String.format(
              "File %s for is already open for writing (type: %s).",
              file.getAbsolutePath(),
              fileType));
    }
    if (!file.exists()) {
      try {
        if (!file.getParentFile().isDirectory() && !file.getParentFile().mkdirs()) {
          throw new IOException(
              String.format(
                  "Failed to create shuffle file directory at"
                      + file.getParentFile().getAbsolutePath() + "(type: %s).", fileType));
        }

        if (!file.createNewFile()) {
          throw new IOException(
              String.format(
                  "Failed to create shuffle file (type: %s).", fileType));
        }
      } catch (IOException e) {
        throw new RuntimeException(
            String.format(
                "Failed to create shuffle file at %s for backup (type: %s).",
                file.getAbsolutePath(),
                fileType),
            e);
      }
    }
    try {
      // TODO encryption
      fileOutputChannel = Channels.newChannel(new FileOutputStream(file));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Failed to find file for writing at %s (type: %s).",
              file.getAbsolutePath(),
              fileType),
          e);
    }
  }

  @Override
  public String getID() {
    return String.format("%s-%d-%d-%s",
        appId,
        shuffleId,
        mapId,
        fileType);
  }

  @Override
  public void onData(String streamId, ByteBuffer buf) throws IOException {
    verifyShuffleFileOpenForWriting();
    while (buf.hasRemaining()) {
      fileOutputChannel.write(buf);
    }
  }

  @Override
  public void onComplete(String streamId) throws IOException {
    if (startTime != 0) {
      logger.info("METRICS: Streaming file " + file.getName() + " took " + (System.nanoTime() - startTime));
    }
    logger.info(
            "Finished writing {}. File type: {}", file.getAbsolutePath(), fileType);
    fileOutputChannel.close();
    if (startTime != 0) {
      logger.info("METRICS: Writing file " + file.getName() + " took " + (System.nanoTime() - startTime));
    }
    callback.onSuccess(ByteBuffer.allocate(0));
    if (startTime != 0) {
      logger.info("METRICS: Sending write file callback for " + file.getName() + " took " + (System.nanoTime() - startTime));
    }
  }

  @Override
  public void onFailure(String streamId, Throwable cause) throws IOException {
    logger.warn("Failed to write shuffle file at {} (type: %s).",
        file.getAbsolutePath(),
        fileType,
        cause);
    fileOutputChannel.close();
    // TODO delete parent dirs too
    if (!file.delete()) {
      logger.warn(
          "Failed to delete incomplete remote shuffle file at %s (type: %s)",
          file.getAbsolutePath(),
          fileType);
    }
  }

  private void verifyShuffleFileOpenForWriting() {
    if (fileOutputChannel == null) {
      throw new RuntimeException(
          String.format(
              "Shuffle file at %s not open for writing (type: %s).",
              file.getAbsolutePath(),
              fileType));
    }
  }
}
