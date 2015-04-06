package edu.brown.cs.systems.pubsub.client;

import java.io.IOException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import edu.brown.cs.systems.pubsub.PubSubConfig;
import edu.brown.cs.systems.pubsub.client.impl.Callback;
import edu.brown.cs.systems.pubsub.client.impl.PubSubClientImpl;

public class PubSubClientFactory {
  
  private static final PubSubClient DUMMY = new PubSubClient() {
      public void shutdown() {}
      public void awaitTermination() throws InterruptedException {}
      public boolean publish(String topic, Message message) {return false;}
      public boolean publish(byte[] topic, Message message) {return false;}
      public boolean publish(ByteString topic, Message message) {return false;}
      public void subscribe(String topic, Callback<?> callback) {}
      public void subscribe(byte[] topic, Callback<?> callback) {}
      public void subscribe(ByteString topic, Callback<?> callback) {}
      public void unsubscribe(String topic, Callback<?> callback) {}
      public void unsubscribe(byte[] topic, Callback<?> callback) {}
      public void unsubscribe(ByteString topic, Callback<?> callback) {}
    };
  
  public static PubSubClient dummy() {
    return DUMMY;
  }

  public static PubSubClient create() throws IOException {
    return create(PubSubConfig.Server.address(), PubSubConfig.Server.port());
  }

  public static PubSubClient create(String hostname, int port)
      throws IOException {
    return create(hostname, port, PubSubConfig.Client.messageSendBufferSize());
  }

  public static PubSubClient create(String hostname, int port,
      int messageSendBufferSize) throws IOException {
    return create(hostname, port, messageSendBufferSize,
        PubSubConfig.Client.sendBufferSize(),
        PubSubConfig.Client.receiveBufferSize());
  }

  public static PubSubClient create(String hostname, int port,
      int messageSendBufferSize, int sendBufferSize, int receiveBufferSize)
      throws IOException {
    return create(hostname, port, messageSendBufferSize, sendBufferSize,
        receiveBufferSize, PubSubConfig.Client.daemon());
  }

  public static PubSubClient create(String hostname, int port,
      int messageSendBufferSize, int sendBufferSize, int receiveBufferSize,
      boolean daemon) throws IOException {
    return new PubSubClientImpl(hostname, port, messageSendBufferSize,
        sendBufferSize, receiveBufferSize, daemon);
  }

}
