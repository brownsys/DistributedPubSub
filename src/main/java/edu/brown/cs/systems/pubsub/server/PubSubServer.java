package edu.brown.cs.systems.pubsub.server;

import java.io.IOException;

import edu.brown.cs.systems.pubsub.PubSubConfig;
import edu.brown.cs.systems.pubsub.PubSubProtos.Header;

public class PubSubServer {

  final ServerConnection server;
  final Subscriptions subscriptions;
  final Connections connections;
  
  PubSubServer() throws IOException {
    this(PubSubConfig.Server.bindto(), PubSubConfig.Server.port());
  }

  PubSubServer(String hostnameBindTo, int port) throws IOException {
    this.server = new ServerConnection(this, hostnameBindTo, port);
    this.subscriptions = new Subscriptions(this);
    this.connections = new Connections(this);
    
    this.init();
  }

  void init() {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {
        try {
          shutdown();
        } catch (InterruptedException e) {
          System.out.println("Interrupted waiting for server to close");
        }
      }
    }));

    server.start();
  }
  
  public void shutdown() throws InterruptedException {
    if (server.isAlive()) {
      System.out.println("Interrupting server thread");
      server.interrupt();
      server.join();
    }
  }
  
  void route(ConnectedClient from, Header header, byte[] message) {
    switch (header.getMessageType()) {
    case PUBLISH:
      if (header.hasTopic()) {
        System.out.println("Published on topic " + header.getTopic().toStringUtf8());
        System.out.println(subscriptions.subscribers(header.getTopic()).size() + " subscribers");
        for (ConnectedClient subscriber : subscriptions.subscribers(header.getTopic())) {
          subscriber.publish(message);
        }
      }
      break;
    case SUBSCRIBE:
      System.out.println("Subscribing " + header.getTopic().toStringUtf8());
      if (header.hasTopic()) {
        subscriptions.subscribe(from, header.getTopic());
      }
      break;
    case UNSUBSCRIBE:
      if (header.hasTopic()) {
        subscriptions.unsubscribe(from, header.getTopic());
      }
      break;
    }
  }
  
  public static void main(String[] args) throws IOException,
      InterruptedException {
    new PubSubServer();
  }
}
