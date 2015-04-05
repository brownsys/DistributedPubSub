package edu.brown.cs.systems.pubsub.server;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;

import com.google.common.collect.Lists;

import edu.brown.cs.systems.pubsub.PubSubProtos.Header;

class ConnectedClient {

  final PubSubServer pubsub;
  final SocketChannel channel;
  private final Incoming incoming;
  private final Outgoing outgoing;

  ConnectedClient(PubSubServer pubsub, SocketChannel channel) {
    this.pubsub = pubsub;
    this.channel = channel;
    this.incoming = new Incoming();
    this.outgoing = new Outgoing();
  }

  void publish(byte[] message) {
    outgoing.enqueue(message);
    pubsub.server.interestRW(channel);
  }

  void read() {
    incoming.read();
  }

  void flush() {
    outgoing.flush();
  }

  /**
   * Buffers incoming data from a connected client. Once a complete message is
   * read, it gets passed to the route method
   */
  class Incoming {

    private final ByteBuffer incomingHeader = ByteBuffer.allocate(4);
    private ByteBuffer incomingMessage = null;

    /** Read everything available from the channel. */
    void read() {
      try {
        // For now, keep reading as much as possible
        while (!Thread.currentThread().isInterrupted()) {
          // Start or continue reading the size prefix
          while (incomingHeader.hasRemaining()) {
            System.out.println("Reading into header, has " + incomingHeader.remaining());
            int numRead = channel.read(incomingHeader);
            System.out.println("Read " + numRead);
            if (numRead == 0) {
              return;
            } else if (numRead == -1) {
              pubsub.connections.close(ConnectedClient.this);
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
              pubsub.connections.close(ConnectedClient.this);
              return;
            }
          }

          // Parse the header, route the message
          incomingMessage.rewind();
          int headerSize = incomingMessage.getInt();
          byte[] message = incomingMessage.array();
          Header header = Header.parseFrom(new ByteArrayInputStream(message, incomingMessage.position(), headerSize));
          pubsub.route(ConnectedClient.this, header, message);

          incomingMessage = null;
          incomingHeader.clear();
          System.out.println("Read complete message");
        }
      } catch (IOException e) {
        System.out.println("IOException routing");
        e.printStackTrace();
        pubsub.connections.close(ConnectedClient.this);
        return;
      } catch (BufferUnderflowException e) {
        System.out.println("Bad message");
        e.printStackTrace();
        pubsub.connections.close(ConnectedClient.this);
        return;
      }
    }
  }

  /**
   * Buffers outgoing messages for a connected client
   */
  class Outgoing {

    private final Queue<byte[]> pending = Lists.newLinkedList();
    private final ByteBuffer outgoingHeader = ByteBuffer.allocate(4);
    private ByteBuffer outgoingMessage = null;

    void enqueue(byte[] message) {
      pending.add(message);
    }

    boolean hasRemaining() {
      return outgoingMessage != null || !pending.isEmpty();
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
            if (numWritten == 0) {
              pubsub.server.interestRW(channel);
              return;
            }
          }

          // Write as much of the message as possible
          while (outgoingMessage.hasRemaining()) {
            int numWritten = channel.write(outgoingMessage);
            if (numWritten == 0) {
              pubsub.server.interestRW(channel);
              return;
            }
          }

          // Done, clear the message
          outgoingMessage = null;
          System.out.println("Wrote complete message");
        }
        if (hasRemaining()) {
          pubsub.server.interestRW(channel);
        } else {
          pubsub.server.interestR(channel);
        }
      } catch (IOException e) {
        pubsub.connections.close(ConnectedClient.this);
      }
    }

  }
}
