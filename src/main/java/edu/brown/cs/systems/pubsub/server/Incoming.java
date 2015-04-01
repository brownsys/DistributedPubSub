package edu.brown.cs.systems.pubsub.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Buffers incoming data from a connected client. Once a complete message is
 * read, it gets passed to the route method
 */
abstract class Incoming {

	final SocketChannel channel; // channel we are reading from
	private boolean closed;
	private final ByteBuffer headerBuffer; // used to read the header
	private ByteBuffer incomingMessage; // current message being read, if
										// any

	Incoming(SocketChannel channel) {
		this.channel = channel;
		this.headerBuffer = ByteBuffer.allocate(4);
		this.closed = false;
	}
	
	abstract void onMessage(byte[] message);

	// Is the channel closed
	boolean isClosed() {
		return closed;
	}

	// Read everything available from the channel.
	void read() {
		try {
			// For now, keep reading as much as possible
			while (!closed) {
				// Start or continue reading the header
				while (headerBuffer.hasRemaining()) {
					int numRead = channel.read(headerBuffer);
					if (numRead == 0) {
						return;
					} else if (numRead == -1) {
						closed = true;
						return;
					}
				}
	
				// Message size is read, but haven't started reading message
				if (incomingMessage == null) {
					headerBuffer.rewind();
					int size = headerBuffer.getInt();
					incomingMessage = ByteBuffer.allocate(size);
				}
	
				// Read into the buffer
				while (!incomingMessage.hasRemaining()) {
					int numRead = channel.read(incomingMessage);
					if (numRead == 0) {
						return;
					} else if (numRead == -1) {
						closed = true;
						return;
					}
				}
	
				onMessage(incomingMessage.array());
				incomingMessage = null;
				headerBuffer.rewind();
			}
		} catch (IOException e) {
			closed = true;
		}
	}
}