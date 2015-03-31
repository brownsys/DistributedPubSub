package edu.brown.cs.systems.pubsub.server;

import java.nio.channels.SocketChannel;

public class Client {
	
	final SocketChannel channel;
	final InMessageBuffer in;
	final OutMessageBuffer out;
	final MessageRouter router;
	
	public Client(SocketChannel channel, MessageRouter router) {
		this.channel = channel;
		this.in = new InMessageBuffer(this);
		this.out = new OutMessageBuffer(this);
		this.router = router;
	}
	
	public void in(byte[] message) {
		router.route(this, message);
	}
	
	public void out(byte[] message) {
		out.write(message);
	}

}
