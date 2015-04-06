package edu.brown.cs.systems.pubsub.server.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;

class ServerThread extends Thread {

  // PubSub
  final PubSubServerImpl pubsub;

  // Server information
  final String host;
  final int port;
  final InetSocketAddress address;

  // Server socket variables
  final int rcvBufferSize;
  final int sndBufferSize;
  private final ServerSocketChannel server;
  private final Selector selector;

  ServerThread(PubSubServerImpl pubsub, String host, int port, int rcvBufferSize, int sndBufferSize) throws IOException {
    this.pubsub = pubsub;

    // Determine bind address
    this.host = host;
    this.port = port;
    this.address = new InetSocketAddress(host, port);
    System.out.printf("Resolved %s:%d to %s\n", host, port, address);

    // Create the server
    this.rcvBufferSize = rcvBufferSize;
    this.sndBufferSize = sndBufferSize;
    this.server = ServerSocketChannel.open();
    this.server.configureBlocking(false);
    this.server.socket().bind(address);
    this.server.socket().setReceiveBufferSize(rcvBufferSize);
    this.selector = SelectorProvider.provider().openSelector();
    this.server.register(this.selector, SelectionKey.OP_ACCEPT);
  }

  @Override
  public void run() {
    System.out.println("Server thread starting");
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
            } else if (key.isReadable()) {
              ((ServerConnection) key.attachment()).read();
            } else if (key.isWritable()) {
              ((ServerConnection) key.attachment()).flush();
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println("IOException in main server loop");
      e.printStackTrace();
    }
    System.out.println("Terminating pubsub server");
    try {
      pubsub.connections.closeAll();
      server.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void accept(SelectionKey key) throws IOException {
    try {
      ServerSocketChannel serverchannel = (ServerSocketChannel) key.channel();
      SocketChannel channel = serverchannel.accept();
      channel.configureBlocking(false);
      channel.socket().setSendBufferSize(sndBufferSize);
      channel.socket().setReceiveBufferSize(rcvBufferSize);
      ServerConnection client = pubsub.connections.register(channel);
      channel.register(selector, SelectionKey.OP_READ, client);
      System.out.println("Accepted new connection");
    } catch (IOException e) {
      Thread.currentThread().interrupt(); // abort server
    }
  }

  void close(SocketChannel channel) {
    channel.keyFor(selector).cancel();
    try {
      channel.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  void interestR(SocketChannel channel) {
    channel.keyFor(selector).interestOps(SelectionKey.OP_READ);

  }

  void interestRW(SocketChannel channel) {
    channel.keyFor(selector).interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
  }

}