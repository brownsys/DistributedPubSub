package edu.brown.cs.systems.pubsub.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Map;

import com.google.common.collect.Maps;

public class PubSubServer {
	
	/** Start a new pubsub server on the specified host and port 
	 * @throws IOException */
	public static PubSubServer start(String host, int port) throws IOException {
		PubSubServer server = new PubSubServer(host, port);
		server.start(false);
		return server;
	}
	
	public static PubSubServer startDaemon(String host, int port) throws IOException {
		PubSubServer server = new PubSubServer(host, port);
		server.start(true);
		return server;
	}
	
	// Server information
	public final String host;
	public final int port;
	public final InetSocketAddress address;
	
	// Server channel
	private ServerSocketChannel server;

	// Selector over all channels
	private Selector selector;
	
	// Maintains subscription information
	private MessageRouter router = new MessageRouter();
	
	private Map<SocketChannel, Client> clients = Maps.newHashMap();
	
	private final Thread serverThread, shutdownThread;

	private PubSubServer(String host, int port) throws IOException {
		// Determine bind address
		this.host = host;
		this.port = port;
		this.address = new InetSocketAddress(host, port);
		System.out.printf("Resolved %s:%d to %s\n", host, port, address);
		
		// Create the server
		this.server = ServerSocketChannel.open();
		this.server.configureBlocking(false);
		this.server.socket().bind(address);
		
		// Create the message router for server connections
		this.router = new MessageRouter();
		
		// Create the selector
		this.selector = SelectorProvider.provider().openSelector();
		this.server.register(this.selector, SelectionKey.OP_ACCEPT);
		
		// Create the server thread and shutdown hook
		serverThread = new Thread(new RunLoop());
		shutdownThread = new Thread(new ShutdownHook());
	}
	
	private void start(boolean daemon) {
		// Start the server thread
		serverThread.setDaemon(daemon);
		serverThread.start();
		
		// Register the shutdown hook
		Runtime.getRuntime().addShutdownHook(shutdownThread);
	}
	
	public void close() throws InterruptedException {
		System.out.println("Interrupting server thread");
		if (serverThread.isAlive()) {
			serverThread.interrupt();
			serverThread.join();
		}
	}
	
	/** Add a new client on the specified channel 
	 * @throws IOException */
	private void addClient(SocketChannel channel) throws IOException {
		Client c = new Client(channel, router);
		clients.put(channel, c);
		channel.configureBlocking(false);
		channel.keyFor(selector).interestOps(SelectionKey.OP_READ);
	}
	
	/** Remove the specified client 
	 * @throws IOException if something bad happened while closing the client */
	private void removeClient(Client client) throws IOException {
		this.router.unsubscribeAll(client);
		clients.remove(client.channel);
		removeChannel(client.channel);
	}
	
	/** Remove the specified channel, and the client for the channel if it exists */
	private void removeChannel(SocketChannel channel) throws IOException {
		if (clients.containsKey(channel)) {
			removeClient(clients.get(channel));
		} else {
			channel.close();
			channel.keyFor(selector).cancel();
		}
	}
	
	private Client clientFor(SocketChannel channel) {
		return clients.get(channel);
	}
	
	/** Implements the run loop of the pubsub server */
	private class RunLoop extends Thread {
		public void run() {
			System.out.println("RunLoop starting");
			while (!Thread.currentThread().isInterrupted()) {
				try {
					// Wait for an event one of the registered channels
					selector.select();
					
					// If thread is interrupted, close everything
					if (Thread.currentThread().isInterrupted()) {
						continue;
					}
	
					// Iterate over the set of keys for which events are available
					for (SelectionKey key : selector.selectedKeys()) {
						if (!key.isValid()) {
							continue;
						}
						if (key.isAcceptable()) {
							accept(key);
						} else if (key.isReadable()) {
							read(key);
						} else if (key.isWritable()) {
							write(key);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			System.out.println("RunLoop interrupted");
			try {
				server.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Closing open connections");
			for (SocketChannel c : clients.keySet()) {
				try {
					c.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			clients.clear();
		}
	}
	
	/** Implements the shutdown hook of the pubsub server */
	private class ShutdownHook extends Thread {
		public void run() {
			try {
				close();
			} catch (InterruptedException e) {
				System.out.println("Interrupted waiting for server to close");
			}
		}
	}

	/**
	 * Accept a new connection and create a client 
	 */
	private void accept(SelectionKey key) throws IOException {
		ServerSocketChannel serverchannel = (ServerSocketChannel) key.channel();
		addClient(serverchannel.accept());
	}

	/**
	 * Reads from the channel for the provided key
	 * @throws IOException if something happened and closing the client failed
	 */
	private void read(SelectionKey key) throws IOException {
		SocketChannel channel = (SocketChannel) key.channel();
		Client client = clientFor(channel);
		boolean closed = false;
		try {
			closed = client.in.read();
		} catch (IOException e) {
			e.printStackTrace();
			closed = true;
		}
		if (closed)
			removeClient(client);
	}

	/**
	 * Writes to the channel for the provided key
	 * @throws IOException if something happened and closing the client failed
	 */
	private void write(SelectionKey key) throws IOException {
		SocketChannel channel = (SocketChannel) key.channel();
		Client client = clientFor(channel);
		boolean closed = false;
		try {
			boolean hasRemaining = client.out.flush();
			if (hasRemaining) {
				client.channel.keyFor(selector).interestOps(SelectionKey.OP_WRITE);
			}			
		} catch (IOException e) {
			e.printStackTrace();
			closed = true;
		}
		if (closed)
			removeClient(client);
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		PubSubServer server = PubSubServer.start("localhost", 5564);
		Thread.sleep(1000);
	}
}