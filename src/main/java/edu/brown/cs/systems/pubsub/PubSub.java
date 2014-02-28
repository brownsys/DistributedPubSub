package edu.brown.cs.systems.pubsub;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class PubSub {
  private static AtomicInteger seed = new AtomicInteger();
  
  public interface Message extends Serializable {}

  public static abstract class Callback {
    protected abstract void OnMessage(Message m);
  }
  
  public static void Initialize() {
    // For now do nothing, but this triggers static initialization
  }
  
  public static void publish(String topic, Message message) {
    PubSubClient.publish(topic, message);
  }
  
  public static void subscribe(String topic, Callback callback) {
    PubSubClient.subscribe(topic, callback);
  }
  
}
