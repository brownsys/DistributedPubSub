package edu.brown.cs.systems.pubsub.client;

import java.io.IOException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import edu.brown.cs.systems.pubsub.PubSubConfig;
import edu.brown.cs.systems.pubsub.PubSubProtos.Header;

public class PubSubClient {

  final boolean daemon;

  final ClientConnection connection;
  final PublishBuffer pending;
  final Subscriptions subscriptions;

  PubSubClient() throws IOException {
    this(PubSubConfig.Server.address(), PubSubConfig.Server.port(), PubSubConfig.Client.messageSendBufferSize(), true);
  }

  PubSubClient(String serverHostName, int serverPort, int maxPendingBytes, boolean daemon) throws IOException {
    this.daemon = daemon;
    this.connection = new ClientConnection(this, serverHostName, serverPort);
    this.pending = new PublishBuffer(maxPendingBytes);
    this.subscriptions = new Subscriptions();
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
          System.out.println("PubSubClient ShutdownHook was " + "interrupted while waiting for ClientConnection termination");
        }
      }
    });
  }

  void close() throws InterruptedException {
    if (connection.isAlive()) {
      System.out.println("Interrupting ClientConnection");
      connection.interrupt();
      connection.join();
    }
  }

  public boolean publish(String topic, Message message) {
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

  public synchronized void subscribe(String topic, Callback<?> callback) {
    // Add the subscription
    boolean first = subscriptions.add(ByteString.copyFromUtf8(topic), callback);

    // If it was the first subscription, send a subscribe message
    if (!first)
      return;

    byte[] bytes = WireFormat.subscribe(topic);
    if (bytes == null) {
      subscriptions.remove(ByteString.copyFromUtf8(topic), callback);
      return;
    }

    pending.force(bytes);
  }

  public synchronized void unsubscribe(String topic, Callback<?> callback) {
    // Remove the subscription
    boolean last = subscriptions.remove(ByteString.copyFromUtf8(topic), callback);

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
