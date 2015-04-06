package edu.brown.cs.systems.pubsub;

import java.io.IOException;

import com.google.protobuf.Message;

import edu.brown.cs.systems.pubsub.Subscriber.Callback;
import edu.brown.cs.systems.pubsub.client.PubSubClient;
import edu.brown.cs.systems.pubsub.client.PubSubClientFactory;

/**
 * Provides static pubsub methods. By default communicates with pubsub server
 * configured in application.conf
 */
public class PubSub {

  private static PubSubClient client;

  private static PubSubClient client() {
    if (client == null) {
      synchronized (PubSub.class) {
        if (client == null) {
          try {
            client = PubSubClientFactory.create();
          } catch (IOException e) {
            System.out.println("Error: could not create pubsubclient");
            e.printStackTrace();
            client = PubSubClientFactory.dummy();
          }
        }
      }
    }
    return client;
  }

  /**
   * Shut down pubsub
   */
  public static synchronized void shutdown() {
    if (client != null) {
      synchronized (PubSub.class) {
        if (client != null) {
          client.shutdown();
          client = PubSubClientFactory.dummy();
        }
      }
    }
  }

  /**
   * Subscribes to the specified topic, registering the provided callback, using
   * the default subscriber.
   * 
   * @param topic
   *          the topic to subscribe to
   * @param callback
   *          the callback to register
   */
  public static void subscribe(String topic, Callback<?> callback) {
    client().subscribe(topic, callback);
  }

  /**
   * Unsubscribes the provided callback from the specified topic
   * 
   * @param topic
   *          the topic to unsubscribe from
   * @param callback
   *          the callback to unsubscribe, if subscribed
   */
  public static void unsubscribe(String topic, Callback<?> callback) {
    client().unsubscribe(topic, callback);
  }

  /**
   * Publishes to the specified topic and message using the default publisher
   * 
   * @param topic
   *          the topic to publish on
   * @param message
   *          the message to publish
   */
  public static void publish(String topic, Message message) {
    client().publish(topic, message);
  }

  /**
   * Subscribes to the specified topic, registering the provided callback, using
   * the default subscriber.
   * 
   * @param topic
   *          the topic to subscribe to
   * @param callback
   *          the callback to register
   */
  public static void subscribe(byte[] topic, Callback<?> callback) {
    client().subscribe(topic, callback);
  }

  /**
   * Publishes to the specified topic and message using the default publisher
   * 
   * @param topic
   *          the topic to publish on
   * @param message
   *          the message to publish
   */
  public static void publish(byte[] topic, Message message) {
    client().publish(topic, message);
  }

}
