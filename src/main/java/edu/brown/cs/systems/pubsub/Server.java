package edu.brown.cs.systems.pubsub;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;

import edu.brown.cs.systems.pubsub.PubSubProtos.Header;

public class Server {

  public static Server start(String host, int port, boolean daemon)
      throws IOException {
    Server server = new Server(host, port);
    server.start(daemon);
    return server;
  }

  // Server information
  public final String host;
  public final int port;
  public final InetSocketAddress address;

  // Server socket variables
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

  private Server(String host, int port) throws IOException {
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
    try {
      while (!Thread.currentThread().isInterrupted()) {
        System.out.println("Selecting");
        selector.select();

        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
          SelectionKey key = it.next();
          it.remove();
          if (key.isValid()) {
            if (key.isAcceptable()) {
              accept(key);
            }
            if (key.isReadable()) {
              read(key);
            } else if (key.isWritable()) {
              write(key);
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println("IOException in main server loop");
      e.printStackTrace();
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
        c.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    connections.clear();
  }

  private void accept(SelectionKey key) throws IOException {
    try {
      ServerSocketChannel serverchannel = (ServerSocketChannel) key.channel();
      SocketChannel channel = serverchannel.accept();
      channel.configureBlocking(false);
      channel.register(selector, SelectionKey.OP_READ);
      connections.register(channel);
      System.out.println("Accepted new connection");
    } catch (IOException e) {
      Thread.currentThread().interrupt(); // abort server
    }
  }

  private void read(SelectionKey key) {
    connections.incoming(key).read();
  }

  private void write(SelectionKey key) {
    connections.outgoing(key).flush();
    doInterestOps(key);
  }

  private void remove(SocketChannel channel) {
    connections.unregister(channel);
    channel.keyFor(selector).cancel();
    try {
      channel.close();
    } catch (IOException e) {
    }
  }

  private void doInterestOps(SocketChannel channel) {
    Outgoing out = connections.outgoing(channel);
    if (out != null) {
      int ops = SelectionKey.OP_READ;
      if (out.hasRemaining())
        ops |= SelectionKey.OP_WRITE;
      out.channel.keyFor(selector).interestOps(ops);
    }
  }

  private void doInterestOps(SelectionKey key) {
    Outgoing out = connections.outgoing(key);
    if (out != null) {
      int ops = SelectionKey.OP_READ;
      if (out.hasRemaining())
        ops |= SelectionKey.OP_WRITE;
      out.channel.keyFor(selector).interestOps(ops);
    }
  }

  private void route(SocketChannel from, Header header, byte[] message) {
    switch (header.getMessageType()) {
    case PUBLISH:
      if (header.hasTopic()) {
        System.out.println("Published on topic " + header.getTopic().toStringUtf8());
        for (Outgoing out : connections.subscribers(header.getTopic())) {
          out.enqueue(message);
          doInterestOps(out.channel);
        }
      }
      break;
    case SUBSCRIBE:
      if (header.hasTopic()) {
        connections.subscribe(from, header.getTopic());
      }
      break;
    case UNSUBSCRIBE:
      if (header.hasTopic()) {
        connections.unsubscribe(from, header.getTopic());
      }
      break;
    }
  }

  /** Maintains connected clients and subscriptions */
  private class Connections {

    final Map<SocketChannel, Incoming> incomingBuffers = Maps.newHashMap();
    final Map<SocketChannel, Outgoing> outgoingBuffers = Maps.newHashMap();
    final Multimap<ByteString, SocketChannel> topics = HashMultimap.create();
    final Multimap<SocketChannel, ByteString> subscribed = HashMultimap
        .create();

    void register(SocketChannel channel) {
      incomingBuffers.put(channel, new Incoming(channel));
      outgoingBuffers.put(channel, new Outgoing(channel));
    }

    void unregister(SocketChannel channel) {
      incomingBuffers.remove(channel);
      outgoingBuffers.remove(channel);
      for (ByteString topic : subscribed.removeAll(channel)) {
        topics.remove(topic, channel);
      }
    }

    void subscribe(SocketChannel channel, ByteString topic) {
      topics.put(topic, channel);
      subscribed.put(channel, topic);
    }

    void unsubscribe(SocketChannel channel, ByteString topic) {
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

    Set<Outgoing> subscribers(ByteString topic) {
      Set<Outgoing> subscribers = Sets.newHashSet();
      for (SocketChannel channel : topics.get(topic)) {
        Outgoing out = outgoingBuffers.get(channel);
        if (out != null)
          subscribers.add(out);
      }
      return subscribers;
    }

  }

  /**
   * Buffers incoming data from a connected client. Once a complete message is
   * read, it gets passed to the route method
   */
  class Incoming {

    final SocketChannel channel;
    private final ByteBuffer incomingHeader = ByteBuffer.allocate(4);
    private ByteBuffer incomingMessage = null;

    Incoming(SocketChannel channel) {
      this.channel = channel;
    }

    /** Read everything available from the channel. */
    void read() {
      try {
        // For now, keep reading as much as possible
        while (!Thread.currentThread().isInterrupted()) {
          // Start or continue reading the size prefix
          while (incomingHeader.hasRemaining()) {
            System.out.println("Reading into header, has "
                + incomingHeader.remaining());
            int numRead = channel.read(incomingHeader);
            System.out.println("Read " + numRead);
            if (numRead == 0) {
              return;
            } else if (numRead == -1) {
              remove(channel);
              return;
            }
          }

          // Message size is read, but haven't started reading message
          if (incomingMessage == null) {
            incomingHeader.rewind();
            int size = incomingHeader.getInt();
            incomingMessage = ByteBuffer.allocate(size);
          }

          System.out.println("Reading size of " + incomingMessage.remaining());

          // Read into the buffer
          while (incomingMessage.hasRemaining()) {
            int numRead = channel.read(incomingMessage);
            System.out.println("Read " + numRead);
            if (numRead == 0) {
              return;
            } else if (numRead == -1) {
              remove(channel);
              return;
            }
          }

          // Parse the header, route the message
          incomingMessage.rewind();
          int headerSize = incomingMessage.getInt();
          byte[] message = incomingMessage.array();
          Header header = Header.parseFrom(new ByteArrayInputStream(message,
              incomingMessage.position(), headerSize));
          route(channel, header, message);

          incomingMessage = null;
          incomingHeader.clear();
          System.out.println("Read complete message");
        }
      } catch (IOException e) {
        System.out.println("IOException routing");
        e.printStackTrace();
        remove(channel);
        return;
      } catch (BufferUnderflowException e) {
        System.out.println("Bad message");
        e.printStackTrace();
        remove(channel);
        return;
      }
    }
  }

  /**
   * Buffers outgoing messages for a connected client
   */
  class Outgoing {

    final SocketChannel channel;
    private final Queue<byte[]> pending = Lists.newLinkedList();
    private final ByteBuffer outgoingHeader = ByteBuffer.allocate(4);
    private ByteBuffer outgoingMessage = null;

    Outgoing(SocketChannel channel) {
      this.channel = channel;
    }

    void enqueue(byte[] message) {
      pending.add(message);
    }

    boolean hasRemaining() {
      return outgoingMessage != null;
    }

    void flush() {
      try {
        // Keep writing until can't write any more
        while (!Thread.currentThread().isInterrupted() && !pending.isEmpty()) {
          // Take the next message and set the header bytes
          if (outgoingMessage == null) {
            outgoingMessage = ByteBuffer.wrap(pending.poll());
            outgoingHeader.position(0);
            outgoingHeader.putInt(outgoingMessage.remaining());
            outgoingHeader.rewind();
          }

          // Write as much of the header as possible
          while (outgoingHeader.hasRemaining()) {
            int numWritten = channel.write(outgoingHeader);
            if (numWritten == 0)
              return;
          }

          // Write as much of the message as possible
          while (outgoingMessage.hasRemaining()) {
            int numWritten = channel.write(outgoingMessage);
            if (numWritten == 0)
              return;
          }

          // Done, clear the message
          outgoingMessage = null;
          System.out.println("Wrote complete message");
        }
      } catch (IOException e) {
        remove(channel);
      }
    }

  }

  public static void main(String[] args) throws IOException,
      InterruptedException {
    Server server = Server.start("localhost", 5564, false);
  }
}