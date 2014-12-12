package edu.brown.cs.systems.pubsub;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

public class Server extends Thread {

  public final String address_in;
  public final String address_out;
  
  /**
   * Creates a new server using the default settings taken from the application config.
   */
  public Server() {
    this(Settings.SERVER_BIND_ADDRESS, Settings.CLIENT_SUBSCRIBE_PORT, Settings.CLIENT_PUBLISH_PORT);
  }
  
  /**
   * Interrupts the pub sub server thread, telling it to stop
   */
  public void shutdown() {
    this.interrupt();
  }

  /**
   * Creates a new server using the specified server settings
   * @param bind_address The address to bind the server to (default 0.0.0.0; all interfaces)
   * @param client_subscribe_port The port for clients to subscribe to 
   * @param client_publish_port The port for clients to publish to
   */
  public Server(String bind_address, int client_subscribe_port, int client_publish_port) {
    address_in = String.format("tcp://%s:%d", bind_address, client_publish_port);
    address_out = String.format("tcp://%s:%d", bind_address, client_subscribe_port);
  }

  @Override
  public void run() {    
    Context zmq = ZMQ.context(1);
    
    // Create subscriber
    Socket subscriber = zmq.socket(ZMQ.SUB);
    subscriber.bind(address_in);
    subscriber.subscribe("".getBytes());
    
    // Create publisher
    Socket publisher = zmq.socket(ZMQ.PUB);
    publisher.setHWM(Settings.OUTGOING_MESSAGE_BUFFER_SIZE);
    publisher.bind(address_out);

    // Print status
    System.out.println("PubSub server messages in @ " + address_in);
    System.out.println("PubSub server messages out @ " + address_out);
    
    // Proxy messages
    ZMQ.proxy(subscriber, publisher, null);
    
    subscriber.close();
    publisher.close();
    zmq.close();
  }

  public static void main(String[] args) throws InterruptedException {
    Server server = new Server(Settings.SERVER_BIND_ADDRESS, Settings.CLIENT_SUBSCRIBE_PORT, Settings.CLIENT_PUBLISH_PORT);
    server.start();
  }

}
