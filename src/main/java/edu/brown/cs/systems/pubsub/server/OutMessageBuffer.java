package edu.brown.cs.systems.pubsub.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;

import com.google.common.collect.Queues;

public class OutMessageBuffer {

	final SocketChannel channel; // channel we are writing to
	private final ByteBuffer outgoingHeader; // header for the outgoing message
	private ByteBuffer outgoingMessage; // wrapped bytes for the outgoing
										// message
	private final Queue<byte[]> pending; // list of pending messages to write

	public OutMessageBuffer(Client client) {
		this.channel = client.channel;
		this.pending = Queues.newArrayDeque();
		this.outgoingHeader = ByteBuffer.allocate(4);
		this.outgoingHeader.position(4);
	}

	/** Add the provided message to the pending outgoing messages */
	public void write(byte[] message) {
		pending.add(message);
	}

	/**
	 * Write as many outgoing messages as possible
	 * 
	 * @return true if there is more data blocked on writing, false otherwise
	 * @throws IOException
	 *             if writing to the underlying channel causes an IOException
	 */
	public boolean flush() throws IOException {
		// Keep writing until can't write any more
		while (!pending.isEmpty()) {
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
					return true;
			}

			// Write as much of the message as possible
			while (outgoingMessage.hasRemaining()) {
				int numWritten = channel.write(outgoingMessage);
				if (numWritten == 0)
					return true;
			}

			// Done, clear the message
			outgoingMessage = null;
		}

		// No more messages to write and did not encounter closed stream
		return false;
	}

}
