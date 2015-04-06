package edu.brown.cs.systems.pubsub.client.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;

class ClientThread extends Thread {

  // Client interacting with this connection
  final PubSubClientImpl client;

  // Reconnection parameters
  private static final int MAX_RECONNECT_INTERVAL = 10000;
  private static final int MIN_RECONNECT_INTERVAL = 0;
  private int reconnect = MIN_RECONNECT_INTERVAL;

  // PubSubServer information
  final String host;
  final int port;
  final int sndBufferSize;
  final int rcvBufferSize;

  // Local client variables
  final Selector selector;

  ClientThread(PubSubClientImpl client, String serverHost, int serverPort, int sendBufferSize, int receiveBufferSize) throws IOException {
    this.client = client;
    this.host = serverHost;
    this.port = serverPort;
    this.sndBufferSize = sendBufferSize;
    this.rcvBufferSize = receiveBufferSize;
    this.selector = SelectorProvider.provider().openSelector();
  }

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        // If we have a reconnect timeout, wait for it
        Thread.sleep(reconnect);

        SocketChannel channel = null;
        try {
          channel = SocketChannel.open();
          channel.socket().setSendBufferSize(sndBufferSize);
          channel.socket().setReceiveBufferSize(rcvBufferSize);
          System.out.println("Attempting connect to " + host + ":" + port);
          channel.connect(new InetSocketAddress(host, port));
          channel.configureBlocking(false);

          // On success reset the reconnect timeout and enter connection loop
          reconnect = 0;
          new ClientConnection(client, selector, channel).run();

        } catch (IOException e) {
          // IOException while trying to establish connection
          System.out.println("IOException in reconnect loop");
          e.printStackTrace();
          reconnect = Math.min(MAX_RECONNECT_INTERVAL, reconnect + 500);

        } finally {
          if (channel != null) {
            try {
              channel.close();
            } catch (IOException e) {
              System.out.println("IOException closing channel");
              e.printStackTrace();
            }
          }
        }
      }
    } catch (InterruptedException e) {
      System.out.println("Client thread interrupted, exiting");
    }
  }

}
