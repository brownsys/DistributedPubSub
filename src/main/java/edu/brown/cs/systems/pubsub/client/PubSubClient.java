package edu.brown.cs.systems.pubsub.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Queues;
import com.google.protobuf.Message;

public class PubSubClient {

  private static final int MAX_RECONNECT_INTERVAL = 10000;
  private static final int MIN_RECONNECT_INTERVAL = 500;
  private int reconnect = MIN_RECONNECT_INTERVAL;

  // Server information
  public final String host;
  public final int port;
  private final Selector selector;

  private ByteBuffer outgoing = null; // the current message being written out

  private final int maxPendingBytes;
  private final AtomicInteger pendingBytes = new AtomicInteger(); // the number
                                                                  // of bytes
                                                                  // queued to
                                                                  // be written
  private final BlockingQueue<byte[]> pendingMessage = Queues
      .newLinkedBlockingQueue(); // the pending bytes to be written

  private PubSubClient(String serverHost, int serverPort, int maxPendingBytes)
      throws IOException {
    // Determine bind address
    this.host = serverHost;
    this.port = serverPort;
    this.selector = SelectorProvider.provider().openSelector();
    this.maxPendingBytes = maxPendingBytes;
  }

  /**
   * Publish a message on a topic
   * 
   * @param topic
   *          the topic to publish the message to
   * @param message
   *          the message to publish
   * @return true if the message was queued for send, false if it cannot be sent
   *         without violating buffer size
   */
  public boolean publish(String topic, Message message) {
    byte[] topicBytes = topic.getBytes();
    int messageLen = message.getSerializedSize();

    // Serialized message size
    int totalSize = 4 // message len
        + 1 // op type
        + 4 // topic len
        + topicBytes.length // topic
        + 4 // messageLen
        + messageLen; // message

    if (pendingBytes.getAndAdd(totalSize) > maxPendingBytes) {
      pendingBytes.getAndAdd(-totalSize);
      return false;
    }
    
    ByteBuffer buf = ByteBuffer.allocate(totalSize);
    buf.putInt(totalSize);
  }

  private void awaitReconnect() throws InterruptedException {
    Thread.sleep(reconnect);
    reconnect *= 2;
  }

  private void reconnectLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        SocketChannel channel;
        try {
          // Resolve and connect to the server in blocking mode, then transition
          // to non-blocking
          channel = SocketChannel.open();
          channel.connect(new InetSocketAddress(host, port));
          channel.configureBlocking(false);
          channel.keyFor(selector).interestOps(
              SelectionKey.OP_READ & SelectionKey.OP_WRITE);
        } catch (IOException e) {
          e.printStackTrace();
          awaitReconnect();
          continue;
        }

        connectedLoop(channel);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

  private void connectedLoop(SocketChannel channel) {
    // Work on this connection until disconnected
    try {
      while (!Thread.currentThread().isInterrupted() && channel.isConnected()) {
        checkPending();

        selector.select();

        for (SelectionKey key : selector.selectedKeys()) {
          if (!key.isValid()) {
            continue;
          }
          if (key.isReadable()) {
            read();
          } else if (key.isWritable()) {
            write();
          }
        }
      }
    } catch (IOException e) {
      try {
        channel.keyFor(selector).cancel();
        channel.close();
      } catch (IOException e1) {
      }
    }
  }

  /** Process pending writes */
  private void checkPending() {

  }

  /** Read available bytes */
  private void read() {

  }

  /** Write bytes */
  private void write() {

  }

  // public void run() {
  // while (true) {
  // try {
  // // Process any pending changes
  // synchronized (this.pendingChanges) {
  // Iterator changes = this.pendingChanges.iterator();
  // while (changes.hasNext()) {
  // ChangeRequest change = (ChangeRequest) changes.next();
  // switch (change.type) {
  // case ChangeRequest.CHANGEOPS:
  // SelectionKey key = change.socket.keyFor(this.selector);
  // key.interestOps(change.ops);
  // break;
  // case ChangeRequest.REGISTER:
  // change.socket.register(this.selector, change.ops);
  // break;
  // }
  // }
  // this.pendingChanges.clear();
  // }
  //
  // // Wait for an event one of the registered channels
  // this.selector.select();
  //
  // // Iterate over the set of keys for which events are available
  // Iterator selectedKeys = this.selector.selectedKeys().iterator();
  // while (selectedKeys.hasNext()) {
  // SelectionKey key = (SelectionKey) selectedKeys.next();
  // selectedKeys.remove();
  //
  // if (!key.isValid()) {
  // continue;
  // }
  //
  // // Check what event is available and deal with it
  // if (key.isConnectable()) {
  // this.finishConnection(key);
  // } else if (key.isReadable()) {
  // this.read(key);
  // } else if (key.isWritable()) {
  // this.write(key);
  // }
  // }
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }
  // }

}
