package edu.brown.cs.systems.pubsub;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/** Loads config values using typesafe config */
public class PubSubConfig {
	
	public final Server server;
	public final Client client;
	
	/** Load the default pubsub config */
	public PubSubConfig() {
		this(ConfigFactory.load().getConfig("pubsub"));
	}
	
	/** Create a pubsub config from the provided typesafe config */
	public PubSubConfig(Config config) {
		this(config.getConfig("server"), config.getConfig("client"));
	}

	/** Create a pubsub config from the provided typesafe configs */
	public PubSubConfig(Config serverConfig, Config clientConfig) {
		this.server = new Server(serverConfig);
		this.client = new Client(clientConfig);
	}
	
	/** PubSub server config */
	public static class Server {
		
		public final String address;
		public final int port;
		public final String bindto;
		public final int receiveBufferSize;
		public final int sendBufferSize;
		
		public Server(Config config) {
			this.address = config.getString("address");
			this.port = config.getInt("port");
			this.bindto = config.getString("bindto");
			this.receiveBufferSize = config.getInt("receive-buffer-size");
			this.sendBufferSize = config.getInt("send-buffer-size");
		}
		
		/** @return the server address from the default config */
		public static String address() {
			return c().server.address;
		}
		
		/** @return the server port from the default config */
		public static int port() {
			return c().server.port;
		}
		
		/** @return the server bindto address from the default config */
		public static String bindto() {
			return c().server.bindto;
		}
		
		/** @return the server receive buffer size */
		public static int receiveBufferSize() {
			return c().server.receiveBufferSize;
		}
		
		/** @return the server send buffer size */
		public static int sendBufferSize() {
			return c().server.sendBufferSize;
		}
	}

	/** PubSub client config */
	public static class Client {
		
		public final int receiveBufferSize;
		public final int sendBufferSize;
		public final int sendMessageBufferSize;
		public final boolean daemon;
		
		public Client(Config config) {
			this.receiveBufferSize = config.getInt("receive-buffer-size");
			this.sendBufferSize = config.getInt("send-buffer-size");
			this.sendMessageBufferSize = config.getInt("send-message-buffer-size");
			this.daemon = config.getBoolean("daemon");
		}
		
		/** @return the client receive buffer size */
		public static int receiveBufferSize() {
			return c().client.receiveBufferSize;
		}
		
		/** @return the client send buffer size */
		public static int sendBufferSize() {
			return c().client.sendBufferSize;
		}
		
		/** @return the client message send buffer size */
		public static int messageSendBufferSize() {
			return c().client.sendMessageBufferSize;
		}

		/** @return does the client thread run as a daemon. default true */
    public static boolean daemon() {
      return c().client.daemon;
    }
		
	}
	
	private static PubSubConfig INSTANCE;
	
	private static synchronized void createInstance() {
		if (INSTANCE==null) {
			INSTANCE = new PubSubConfig();
		}
	}
	
	private static PubSubConfig c() {
		if (INSTANCE==null) {
			createInstance();
		}
		return INSTANCE;
	}
	
	
    
}
