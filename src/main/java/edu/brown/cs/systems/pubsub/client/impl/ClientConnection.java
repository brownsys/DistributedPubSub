package edu.brown.cs.systems.pubsub.client.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

class ClientConnection implements Runnable {

  final PubSubClientImpl client;
  final Selector selector;
  final SocketChannel channel;
  final InputReader in;
  final OutputWriter out;

  ClientConnection(PubSubClientImpl client, Selector selector, SocketChannel channel) {
    this.client = client;
    this.selector = selector;
    this.channel = channel;
    this.in = new InputReader();
    this.out = new OutputWriter();
  }

  @Override
  public void run() {
    try {
      // Register channel ops with the selector
      doInterestOps(channel);

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
      Thread.currentThread().interrupt();
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

    private final ByteBuffer outgoingHeader = ByteBuffer.allocate(4);
    private ByteBuffer outgoingMessage = null;

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
              return false;
          }

          // Write as much of the message as possible
          while (outgoingMessage.hasRemaining()) {
            int numWritten = channel.write(outgoingMessage);
            if (numWritten == 0)
              return false;
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

    private final ByteBuffer sizePrefixBuffer = ByteBuffer.allocate(4);
    private ByteBuffer incomingMessage = null;

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
              return false;
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
          while (incomingMessage.hasRemaining()) {
            int numRead = channel.read(incomingMessage);
            if (numRead == 0) {
              return false;
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
