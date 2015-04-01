package edu.brown.cs.systems.pubsub.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class PubSubServer {
  
  public static PubSubServer start(String host, int port, boolean daemon) throws IOException {
    PubSubServer server = new PubSubServer(host, port);
    server.start(daemon);
    return server;
  }

  // Server information
  public final String host;
  public final int port;
  public final InetSocketAddress address;
  private final ServerSocketChannel server;
  private final Selector selector;

  // Manages connections and subscriptions
  private final Connections connections = new Connections();

  // Thread for run loop of the server
  private final Thread serverThread = new Thread(new Runnable() {
    public void run() {
      serverLoop();
    }
  });

  // Shutdown handler to close server
  private final Thread shutdownThread = new Thread(new Runnable() {
    public void run() {
      try {
        close();
      } catch (InterruptedException e) {
        System.out.println("Interrupted waiting for server to close");
      }
    }
  });

  private PubSubServer(String host, int port) throws IOException {
    // Determine bind address
    this.host = host;
    this.port = port;
    this.address = new InetSocketAddress(host, port);
    System.out.printf("Resolved %s:%d to %s\n", host, port, address);

    // Create the server
    this.server = ServerSocketChannel.open();
    this.server.configureBlocking(false);
    this.server.socket().bind(address);
    this.selector = SelectorProvider.provider().openSelector();
    this.server.register(this.selector, SelectionKey.OP_ACCEPT);
  }

  private void start(boolean daemon) {
    // Start the server thread
    serverThread.setDaemon(daemon);
    serverThread.start();

    // Register the shutdown hook
    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }

  public void close() throws InterruptedException {
    if (serverThread.isAlive()) {
      System.out.println("Interrupting server thread");
      serverThread.interrupt();
      serverThread.join();
    }
  }

  private void serverLoop() {
    System.out.println("RunLoop starting");
    while (!Thread.currentThread().isInterrupted()) {
      try {
        selector.select();

        for (SelectionKey key : selector.selectedKeys()) {
          if (!key.isValid()) {
            continue;
          }
          if (key.isAcceptable()) {
            accept(key);
          } else if (key.isReadable()) {
            read(key);
          } else if (key.isWritable()) {
            write(key);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    System.out.println("RunLoop interrupted");
    try {
      server.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("Closing open connections");
    for (SocketChannel c : connections.connected()) {
      try {
        c.keyFor(selector).cancel();
        c.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    connections.clear();
  }

  // Accept a new connection from a client
  private void accept(SelectionKey key) throws IOException {
    ServerSocketChannel serverchannel = (ServerSocketChannel) key.channel();

    SocketChannel channel = serverchannel.accept();
    channel.configureBlocking(false);
    channel.keyFor(selector).interestOps(SelectionKey.OP_READ);

    connections.register(channel);
  }

  // Tear down a connection from a client
  private void remove(SocketChannel channel) {
    try {
      channel.keyFor(selector).cancel();
      channel.close();
    } catch (IOException e) {
      connections.unregister(channel);
    }
  }

  /**
   * Reads from the channel for the provided key
   * 
   * @throws IOException
   *           if something happened and closing the client failed
   */
  private void read(SelectionKey key) throws IOException {
    // Read as much as possible
    Incoming in = connections.incoming(key);
    in.read();

    // Check if closed
    if (in.isClosed())
      remove(in.channel);
  }

  /**
   * Writes to the channel for the provided key
   * 
   * @throws IOException
   *           if something happened and closing the client failed
   */
  private void write(SelectionKey key) throws IOException {
    // Flush as much as possible
    Outgoing out = connections.outgoing(key);
    out.flush();

    // Check if closed
    if (out.isClosed()) {
      remove(out.channel);
      return;
    }

    // Register for write
    int ops = SelectionKey.OP_READ;
    if (out.hasRemaining())
      ops &= SelectionKey.OP_WRITE;
    out.channel.keyFor(selector).interestOps(ops);
  }

  private void route(SocketChannel from, byte[] message) {
    // Read the message type and topic
    ByteBuffer buf = ByteBuffer.wrap(message);
    byte type = buf.get();
    int topicLength = buf.getInt();
    byte[] topic = new byte[topicLength];
    buf.get(topic);

    if (type == 0) {
      // Publish
      for (Outgoing out : connections.subscribers(topic)) {
        out.enqueue(message);
        out.channel.keyFor(selector).interestOps(
            SelectionKey.OP_READ & SelectionKey.OP_WRITE);
      }
      
    } else if (type == 1) {
      // Subscribe
      connections.subscribe(from, topic);
      
    } else if (type == 2) {
      // Unsubscribe
      connections.unsubscribe(from, topic);
      
    }
  }

  /** Routes incoming messages to subscribers */
  private class IncomingImpl extends Incoming {
    IncomingImpl(SocketChannel channel) {
      super(channel);
    }

    void onMessage(byte[] message) {
      route(channel, message);
    }
  }

  /** Maintains connected clients and subscriptions */
  private class Connections {

    final Map<SocketChannel, Incoming> incomingBuffers = Maps.newHashMap();
    final Map<SocketChannel, Outgoing> outgoingBuffers = Maps.newHashMap();
    final Multimap<byte[], SocketChannel> topics = HashMultimap.create();
    final Multimap<SocketChannel, byte[]> subscribed = HashMultimap.create();

    void register(SocketChannel channel) {
      incomingBuffers.put(channel, new IncomingImpl(channel));
      outgoingBuffers.put(channel, new Outgoing(channel));
    }

    void unregister(SocketChannel channel) {
      incomingBuffers.remove(channel);
      outgoingBuffers.remove(channel);
      for (byte[] topic : subscribed.removeAll(channel)) {
        topics.remove(topic, channel);
      }
    }

    void subscribe(SocketChannel channel, byte[] topic) {
      topics.put(topic, channel);
      subscribed.put(channel, topic);
    }

    void unsubscribe(SocketChannel channel, byte[] topic) {
      topics.remove(topic, channel);
      subscribed.remove(channel, topic);
    }

    void clear() {
      incomingBuffers.clear();
      outgoingBuffers.clear();
      topics.clear();
      subscribed.clear();
    }

    Set<SocketChannel> connected() {
      return incomingBuffers.keySet();
    }

    Outgoing outgoing(SelectionKey key) {
      return outgoing((SocketChannel) key.channel());
    }

    Outgoing outgoing(SocketChannel channel) {
      return outgoingBuffers.get(channel);
    }

    Incoming incoming(SelectionKey key) {
      return incoming((SocketChannel) key.channel());
    }

    Incoming incoming(SocketChannel channel) {
      return incomingBuffers.get(channel);
    }

    Set<Outgoing> subscribers(byte[] topic) {
      Set<Outgoing> subscribers = Sets.newHashSet();
      for (SocketChannel channel : topics.get(topic)) {
        Outgoing out = outgoingBuffers.get(channel);
        if (out != null)
          subscribers.add(out);
      }
      return subscribers;
    }

  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    PubSubServer server = PubSubServer.start("localhost", 5564, true);
    Thread.sleep(1000);
  }
}