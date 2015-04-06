package edu.brown.cs.systems.pubsub.server.impl;

import java.io.IOException;

import edu.brown.cs.systems.pubsub.PubSubProtos.Header;
import edu.brown.cs.systems.pubsub.server.PubSubServer;

public class PubSubServerImpl implements PubSubServer {
  
  final ServerThread server;
  final ServerSubscriptions subscriptions;
  final ServerConnectionManager connections;

  public PubSubServerImpl(String hostnameBindTo, int port, int rcvBufferSize, int sndBufferSize) throws IOException {
    this.server = new ServerThread(this, hostnameBindTo, port, rcvBufferSize, sndBufferSize);
    this.subscriptions = new ServerSubscriptions(this);
    this.connections = new ServerConnectionManager(this);
    
    this.init();
  }

  void init() {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      public void run() {
        try {
          close();
        } catch (InterruptedException e) {
          System.out.println("Interrupted waiting for server to close");
        }
      }
    }));

    server.start();
  }

  @Override
  public void shutdown() {
    if (server.isAlive()) {
      System.out.println("Interrupting server thread");
      server.interrupt();
    }    
  }

  @Override
  public void awaitTermination() throws InterruptedException {
    if (server.isAlive()) {
      server.join();
    }
  }

  public void close() throws InterruptedException {
    shutdown();
    awaitTermination();
  }
  
  void route(ServerConnection from, Header header, byte[] message) {
    switch (header.getMessageType()) {
    case PUBLISH:
      if (header.hasTopic()) {
        System.out.println("Published on topic " + header.getTopic().toStringUtf8());
        System.out.println(subscriptions.subscribers(header.getTopic()).size() + " subscribers");
        for (ServerConnection subscriber : subscriptions.subscribers(header.getTopic())) {
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
  
}
