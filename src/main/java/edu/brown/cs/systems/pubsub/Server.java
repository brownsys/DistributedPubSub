package edu.brown.cs.systems.pubsub;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class Server extends Thread {

  public final String address_in;
  public final String address_out;

  public Server(String bind_address, int client_subscribe_port, int client_publish_port) {
    address_in = String.format("tcp://%s:%d", bind_address, client_publish_port);
    address_out = String.format("tcp://%s:%d", bind_address, client_subscribe_port);
  }

  @Override
  public void run() {    
    // Create subscriber
    Socket subscriber = PubSub.context.socket(ZMQ.SUB);
    subscriber.bind(address_in);
    subscriber.subscribe("".getBytes());
    
    // Create publisher
    Socket publisher = PubSub.context.socket(ZMQ.PUB);
    publisher.setHWM(Settings.OUTGOING_MESSAGE_BUFFER_SIZE);
    publisher.bind(address_out);

    // Print status
    System.out.println("PubSub server messages in @ " + address_in);
    System.out.println("PubSub server messages out @ " + address_out);
    
    // Proxy messages
    while (!Thread.currentThread().isInterrupted()) {
      byte[] envelope = subscriber.recv(0);
      byte[] bytes = subscriber.recv(0);
      publisher.send(envelope, ZMQ.SNDMORE);
      publisher.send(bytes, 0);
    }

    // Once interrupted, shut ourselves down
    publisher.close();
    subscriber.close();
  }

  public static void main(String[] args) throws InterruptedException {
    Server server = new Server(Settings.SERVER_BIND_ADDRESS, Settings.CLIENT_SUBSCRIBE_PORT, Settings.CLIENT_PUBLISH_PORT);
    server.start();
  }

}
