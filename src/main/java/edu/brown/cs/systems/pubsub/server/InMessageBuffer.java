package edu.brown.cs.systems.pubsub.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Reads messages from a socket channel and once complete messages are read,
 * hands them off to the MessageHandler
 */
public class InMessageBuffer {

	final Client client; // handler for completed messages
	final SocketChannel channel; // channel we are reading from
	private final ByteBuffer headerBuffer; // used to read the header
	private ByteBuffer incomingMessage; // current message being read, if any

	public InMessageBuffer(Client client) {
		this.client = client;
		this.channel = client.channel;
		this.headerBuffer = ByteBuffer.allocate(4);
	}

	/**
	 * Read everything available from the channel. Complete messages will be
	 * passed to the handler
	 * @return true if the connection has been closed, false otherwise
	 * @throws IOException if reading from the channel causes an io exception
	 */
	public boolean read() throws IOException {
		// For now, keep reading as much as possible
		while (true) {
			// Start or continue reading the header
			while (headerBuffer.hasRemaining()) {
				int numRead = channel.read(headerBuffer);
				if (numRead <= 0)
					return numRead == -1;
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
				if (numRead <= 0)
					return numRead == -1;
			}

			client.in(incomingMessage.array());
			incomingMessage = null;
			headerBuffer.rewind();
		}
	}

}
