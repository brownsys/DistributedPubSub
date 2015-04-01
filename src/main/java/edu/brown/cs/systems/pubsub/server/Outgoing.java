package edu.brown.cs.systems.pubsub.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;

import com.google.common.collect.Lists;

/**
 * Buffers outgoing messages for a connected client
 */
class Outgoing {

	final SocketChannel channel; // channel we are writing to
	private boolean closed;
	private final ByteBuffer outgoingHeader; // outgoing message header
	private ByteBuffer outgoingMessage; // outgoing message
	private final Queue<byte[]> pending; // pending messages

	Outgoing(SocketChannel channel) {
		this.channel = channel;
		this.pending = Lists.newLinkedList();
		this.outgoingHeader = ByteBuffer.allocate(4);
		this.outgoingHeader.position(4);
		this.closed = false;
	}

	void enqueue(byte[] message) {
		pending.add(message);
	}

	void flush() throws IOException {
		try {
			// Keep writing until can't write any more
			while (!closed && !pending.isEmpty()) {
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
			}
		} catch (IOException e) {
			closed = true;
		}
	}

	boolean hasRemaining() {
		return outgoingMessage != null;
	}
	
	boolean isClosed() {
		return closed;
	}
}
