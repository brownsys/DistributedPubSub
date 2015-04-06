package edu.brown.cs.systems.pubsub.client.impl;

import java.io.IOException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import edu.brown.cs.systems.pubsub.PubSubProtos.Header;
import edu.brown.cs.systems.pubsub.client.PubSubClient;
import edu.brown.cs.systems.pubsub.WireFormat;

public class PubSubClientImpl implements PubSubClient {

  // PubSubServer information
  public final String host;
  public final int port;

  final boolean daemon;

  final ClientThread connection;
  final ClientPublishBuffer pending;
  final ClientSubscriptions subscriptions;

  public PubSubClientImpl(String serverHostName, int serverPort, int maxPendingBytes,
      int sendBufferSize, int receiveBufferSize, boolean daemon) throws IOException {
    this.host = serverHostName;
    this.port = serverPort;

    this.daemon = daemon;
    this.connection = new ClientThread(this, serverHostName, serverPort, sendBufferSize, receiveBufferSize);
    this.pending = new ClientPublishBuffer(maxPendingBytes);
    this.subscriptions = new ClientSubscriptions();

    this.init();
  }

  void init() {
    connection.setDaemon(daemon);
    connection.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          close();
        } catch (InterruptedException e) {
          System.out.println("PubSubClient ShutdownHook was "
              + "interrupted while waiting for ClientConnection termination");
        }
      }
    });
  }

  @Override
  public void shutdown() {
    if (connection.isAlive()) {
      System.out.println("Interrupting ClientConnection");
      connection.interrupt();
    }    
  }

  @Override
  public void awaitTermination() throws InterruptedException {
    if (connection.isAlive()) {
      connection.join();
    }
  }

  public void close() throws InterruptedException {
    shutdown();
    awaitTermination();
  }

  @Override
  public boolean publish(String topic, Message message) {
    return publish(WireFormat.topic(topic), message);
  }

  @Override
  public boolean publish(byte[] topic, Message message) {
    return publish(WireFormat.topic(topic), message);
  }

  @Override
  public boolean publish(ByteString topic, Message message) {
    // Create the publish message
    byte[] bytes = WireFormat.publish(topic, message);
    if (bytes == null)
      return false;

    // Try to enqueue the published bytes
    boolean added = pending.add(bytes);
    if (added)
      connection.selector.wakeup();
    return added;
  }

  @Override
  public void subscribe(String topic, Callback<?> callback) {
    subscribe(WireFormat.topic(topic), callback);
  }

  @Override
  public void subscribe(byte[] topic, Callback<?> callback) {
    subscribe(WireFormat.topic(topic), callback);
  }
  
  @Override
  public synchronized void subscribe(ByteString topic, Callback<?> callback) {
    // Add the subscription
    boolean first = subscriptions.add(topic, callback);

    // If it was the first subscription, send a subscribe message
    if (!first)
      return;

    byte[] bytes = WireFormat.subscribe(topic);
    if (bytes == null) {
      subscriptions.remove(topic, callback);
      return;
    }

    pending.force(bytes);
  }

  @Override
  public void unsubscribe(String topic, Callback<?> callback) {
    unsubscribe(WireFormat.topic(topic), callback);
  }

  @Override
  public void unsubscribe(byte[] topic, Callback<?> callback) {
    unsubscribe(WireFormat.topic(topic), callback);
  }

  @Override
  public synchronized void unsubscribe(ByteString topic, Callback<?> callback) {
    // Remove the subscription
    boolean last = subscriptions.remove(topic, callback);

    // If it was the last subscription, send an unsubscribe message
    if (!last)
      return;

    byte[] bytes = WireFormat.unsubscribe(topic);
    if (bytes == null)
      return;

    pending.force(bytes);
  }

  // Called by the client connection each time we connect / reconnect to server
  synchronized void OnConnect() {
    // Resubscribe to all subscribed topics
    for (ByteString topic : subscriptions.topics()) {
      byte[] bytes = WireFormat.subscribe(topic);
      if (bytes != null)
        pending.force(bytes);
    }
  }

  // Called by the connection each time a message is received
  synchronized void OnMessage(byte[] message) {
    try {
      Header header = WireFormat.header(message);
      switch (header.getMessageType()) {
      case PUBLISH:
        if (header.hasTopic()) {
          ByteString topic = header.getTopic();
          for (Callback<?> callback : subscriptions.callbacks(topic)) {
            callback.OnMessage(message);
          }
        }
        break;
      default:
        break;
      }
    } catch (Exception e) {

    }
  }

}
