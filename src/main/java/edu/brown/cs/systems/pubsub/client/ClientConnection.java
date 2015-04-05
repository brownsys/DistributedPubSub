package edu.brown.cs.systems.pubsub.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;

class ClientConnection extends Thread {

  // Client interacting with this connection
  final PubSubClient client;

  // Reconnection parameters
  private static final int MAX_RECONNECT_INTERVAL = 10000;
  private static final int MIN_RECONNECT_INTERVAL = 0;
  private int reconnect = MIN_RECONNECT_INTERVAL;

  // PubSubServer information
  public final String host;
  public final int port;

  // Local client variables
  final Selector selector;

  ClientConnection(PubSubClient client, String serverHost, int serverPort) throws IOException {
    this.client = client;
    this.host = serverHost;
    this.port = serverPort;
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
          System.out.println("Attempting connect to " + host + ":" + port);
          channel.connect(new InetSocketAddress(host, port));
          channel.configureBlocking(false);

          // On success reset the reconnect timeout and enter connection loop
          reconnect = 0;
          connectedLoop(channel);

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

  private void connectedLoop(SocketChannel channel) {
    try {
      // Register channel ops with the selector
      doInterestOps(channel);

      // Create message IO for the channel
      InputReader in = new InputReader(channel);
      OutputWriter out = new OutputWriter(channel);

      // Trigger client to resubscribe
      client.OnConnect();

      while (!Thread.currentThread().isInterrupted() && channel.isConnected()) {
        // Wait for R/W to be ready or for a caller to manually wake us
        selector.select();

        // Iterate over selected keys
        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
        while (it.hasNext()) {
          // Remove selected key to reset its ready state
          SelectionKey key = it.next();
          it.remove();
          System.out.println("Key " + key);

          // ignore invalid keys
          if (!key.isValid()) {
            continue;
          }
          System.out.println("Valid");

          // do read IO
          if (key.isReadable()) {
            System.out.println("Readable");
            boolean completed = in.read();
            if (completed)
              return;

            // do write IO
          } else if (key.isWritable()) {
            System.out.println("Writable");
            boolean completed = out.flush();
            if (completed)
              return;
          }
        }

        // Might have written all data; update interest set
        doInterestOps(channel);
      }
    } catch (IOException e) {
      System.out.println("IOException in connectedLoop");
      e.printStackTrace();
    } catch (ClosedSelectorException e) {
      System.out.println("ClientConnection attempt to select on a closed selector");
      e.printStackTrace();
      this.interrupt();
    }
    System.out.println("Connected loop complete");
  }

  private void doInterestOps(SocketChannel channel) throws IOException {
    int ops = SelectionKey.OP_READ;
    if (client.pending.hasMore())
      ops |= SelectionKey.OP_WRITE;
    channel.register(selector, ops);
  }

  class OutputWriter {

    final SocketChannel channel;
    private final ByteBuffer outgoingHeader = ByteBuffer.allocate(4);
    private ByteBuffer outgoingMessage = null;

    OutputWriter(SocketChannel channel) {
      this.channel = channel;
    }

    boolean flush() {
      System.out.println("Doing some flushing");
      try {
        // Keep writing until can't write any more
        while (!Thread.currentThread().isInterrupted() && client.pending.hasMore()) {
          // Take the next message and set the header bytes
          if (outgoingMessage == null) {
            outgoingMessage = ByteBuffer.wrap(client.pending.current());
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
          client.pending.completed();
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
  class InputReader {

    final SocketChannel channel;
    private final ByteBuffer sizePrefixBuffer = ByteBuffer.allocate(4);
    private ByteBuffer incomingMessage = null;

    InputReader(SocketChannel channel) {
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

          client.OnMessage(incomingMessage.array());
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

}
