package edu.brown.cs.systems.pubsub;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.collect.Queues;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

import edu.brown.cs.systems.pubsub.PubSubProtos.Header;
import edu.brown.cs.systems.pubsub.PubSubProtos.StringMessage;

public class Client {

  public static Client connect() throws IOException {
    return connect(PubSubConfig.Server.address(), PubSubConfig.Server.port(),
        PubSubConfig.Client.messageSendBufferSize(), true);
  }

  public static Client connect(String host, int port, int maxBufferSize,
      boolean daemon) throws IOException {
    Client client = new Client(host, port, maxBufferSize);
    client.start(daemon);
    return client;
  }

  private static final int MAX_RECONNECT_INTERVAL = 10000;
  private static final int MIN_RECONNECT_INTERVAL = 0;
  private int reconnect = MIN_RECONNECT_INTERVAL;

  // PubSubServer information
  public final String host;
  public final int port;

  // Local client variables
  private final Selector selector;
  private final int maxPendingBytes;
  private final PendingMessages pending = new PendingMessages();

  // Thread for run loop of the client
  private final Thread clientThread = new Thread(new Runnable() {
    public void run() {
      reconnectLoop();
    }
  });

  // Shutdown handler to close client
  private final Thread shutdownThread = new Thread(new Runnable() {
    public void run() {
      try {
        close();
      } catch (InterruptedException e) {
        System.out.println("Interrupted waiting for client to close");
      }
    }
  });

  private Client(String serverHost, int serverPort, int maxPendingBytes)
      throws IOException {
    // Determine bind address
    this.host = serverHost;
    this.port = serverPort;
    this.selector = SelectorProvider.provider().openSelector();
    this.maxPendingBytes = maxPendingBytes;
  }

  private void start(boolean daemon) {
    // Start the client thread
    clientThread.setDaemon(daemon);
    clientThread.start();

    // Register the shutdown hook
    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }

  public void close() throws InterruptedException {
    if (clientThread.isAlive()) {
      System.out.println("Interrupting client thread");
      clientThread.interrupt();
      clientThread.join();
    }
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
    boolean added = pending.publish(topic, message);
    if (added)
      selector.wakeup();
    return added;
  }

  private void reconnectLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          SocketChannel channel = null;
          try {
            channel = SocketChannel.open();
            System.out.println("Attempting connect to " + host + ":" + port);
            channel.connect(new InetSocketAddress(host, port));
            channel.configureBlocking(false);

            connectedLoop(channel);
          } finally {
            if (channel != null) {
              try {
                channel.close();
              } catch (IOException e) {
                System.out.println("IOException cancelling channel");
                e.printStackTrace();
              }
            }
          }
        } catch (IOException e) {
          System.out.println("IOException in reconnect loop");
          e.printStackTrace();
          Thread.sleep(reconnect);
          reconnect = Math.min(MAX_RECONNECT_INTERVAL, reconnect + 500);
        }
      }
    } catch (InterruptedException e) {
      System.out.println("Client thread interrupted, exiting");
    }
  }

  private void connectedLoop(SocketChannel channel) {
    try {
      doInterestOps(channel);

      Incoming in = new Incoming(channel);
      Outgoing out = new Outgoing(channel);

      System.out.println("Doing connected loop");
      while (!Thread.currentThread().isInterrupted() && channel.isConnected()) {
        System.out.println("Selecting...");
        int count = selector.select();
        System.out.println("Selected! " + count);

        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
          SelectionKey key = it.next();
          it.remove();
          System.out.println("Key " + key);
          if (key.isValid()) {
            System.out.println("Valid");
            if (key.isReadable()) {
              System.out.println("Readable");
              boolean completed = in.read();
              if (completed)
                return;
            } else if (key.isWritable()) {
              System.out.println("Writable");
              boolean completed = out.flush();
              if (completed)
                return;
            }
          }
        }

        doInterestOps(channel);
      }
    } catch (IOException e) {
      System.out.println("IOException in connectedLoop");
      e.printStackTrace();
    }
    System.out.println("Connected loop complete");
  }

  private void doInterestOps(SocketChannel channel)
      throws ClosedChannelException {
    int ops = SelectionKey.OP_READ;
    if (pending.hasPending())
      ops |= SelectionKey.OP_WRITE;
    channel.register(selector, ops);
  }

  private void route(SocketChannel channel, byte[] array) {
    System.out.println("Routing message");
  }

  /**
   * Keeps track of pending outgoing messages
   */
  class PendingMessages {

    private final BlockingQueue<byte[]> pending = Queues
        .newLinkedBlockingQueue();
    private final AtomicInteger size = new AtomicInteger();

    /**
     * Enqueue the provided message Return true if enqueued without violating
     * space restrictions, false otherwise
     */
    public boolean publish(String topic, Message message) {
      System.out.println("Publish topic " + topic);
      Header header = Header.newBuilder()
          .setMessageType(Header.MessageType.PUBLISH)
          .setTopic(ByteString.copyFromUtf8(topic))
          .build();
      
      int headerSize = header.getSerializedSize();
      int messageSize = message.getSerializedSize();
      int totalSize = 4   // header size
          + headerSize    // header
          + messageSize;  // payload
      
      byte[] bytes = new byte[totalSize];
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      buf.putInt(headerSize);
      CodedOutputStream cos = CodedOutputStream.newInstance(bytes, buf.position(), buf.remaining());      
      try {
        header.writeTo(cos);
        message.writeTo(cos);
      } catch (IOException e) {
        e.printStackTrace();
        size.getAndAdd(-totalSize);
        return false;
      }

      pending.add(bytes);
      System.out.println("Added pending");
      return true;
    }

    /** Get the current message */
    public byte[] current() {
      return pending.peek();
    }

    /** Completed send of the current message. Advance to next message */
    public void completed() {
      pending.poll();
    }

    public boolean hasPending() {
      return current() != null;
    }

  }

  /**
   * Buffers outgoing messages for a connected client
   */
  class Outgoing {

    final SocketChannel channel;
    private final ByteBuffer outgoingHeader = ByteBuffer.allocate(4);
    private ByteBuffer outgoingMessage = null;

    Outgoing(SocketChannel channel) {
      this.channel = channel;
    }

    boolean hasRemaining() {
      return outgoingMessage != null;
    }

    boolean flush() {
      System.out.println("Doing some flushing");
      try {
        // Keep writing until can't write any more
        while (!Thread.currentThread().isInterrupted() && pending.hasPending()) {
          // Take the next message and set the header bytes
          if (outgoingMessage == null) {
            outgoingMessage = ByteBuffer.wrap(pending.current());
            outgoingHeader.clear();
            outgoingHeader.putInt(outgoingMessage.remaining());
            System.out.println("Writing message " + outgoingMessage.remaining());
            outgoingHeader.rewind();
          }

          // Write as much of the header as possible
          while (outgoingHeader.hasRemaining()) {
            int numWritten = channel.write(outgoingHeader);
            if (numWritten == 0)
              break;
          }

          // Write as much of the message as possible
          while (outgoingMessage.hasRemaining()) {
            int numWritten = channel.write(outgoingMessage);
            if (numWritten == 0)
              break;
          }

          // Done, clear the message
          outgoingMessage = null;
          pending.completed();
          System.out.println("Wrote complete message");
        }
      } catch (Exception e) {
        return true;
      }
      return false;
    }

  }

  /**
   * Buffers incoming data from a connected client. Once a complete message is
   * read, it gets passed to the route method
   */
  class Incoming {

    final SocketChannel channel;
    private final ByteBuffer sizePrefixBuffer = ByteBuffer.allocate(4);
    private ByteBuffer incomingMessage = null;

    Incoming(SocketChannel channel) {
      this.channel = channel;
    }

    /**
     * Read everything available from the channel. Return true if the connection
     * is completed
     */
    boolean read() {
      try {
        // For now, keep reading as much as possible
        while (!Thread.currentThread().isInterrupted()) {
          // Start or continue reading the size prefix
          while (sizePrefixBuffer.hasRemaining()) {
            int numRead = channel.read(sizePrefixBuffer);
            if (numRead == 0) {
              break;
            } else if (numRead == -1) {
              return true;
            }
          }

          // Message size is read, but haven't started reading message
          if (incomingMessage == null) {
            sizePrefixBuffer.rewind();
            int size = sizePrefixBuffer.getInt();
            incomingMessage = ByteBuffer.allocate(size);
          }

          // Read into the buffer
          while (!incomingMessage.hasRemaining()) {
            int numRead = channel.read(incomingMessage);
            if (numRead == 0) {
              break;
            } else if (numRead == -1) {
              return true;
            }
          }

          route(channel, incomingMessage.array());
          incomingMessage = null;
          sizePrefixBuffer.clear();
          System.out.println("Read complete message");
        }
      } catch (Exception e) {
        e.printStackTrace();
        return true;
      }
      return false;
    }

  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    Client c = Client.connect();
    while (true) {
      c.publish(RandomStringUtils.randomAlphanumeric(3), StringMessage.newBuilder()
          .setMessage(RandomStringUtils.random(100)).build());
      Thread.sleep(1000);
    }
  }

}
