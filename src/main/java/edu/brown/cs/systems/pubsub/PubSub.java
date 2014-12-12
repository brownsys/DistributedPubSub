package edu.brown.cs.systems.pubsub;

import com.google.protobuf.Message;

import edu.brown.cs.systems.pubsub.Subscriber.Callback;

public class PubSub {
  
  private static Subscriber default_subscriber = null;
  private static Publisher default_publisher = null;
  
  public static synchronized void shutdown() {
	  if (default_publisher!=null)
		  default_publisher.close();
	  if (default_subscriber!=null)
		  default_subscriber.close();
  }
  
  private static synchronized void create_default_publisher() {
    if (default_publisher==null)
      default_publisher = new Publisher();
  }
  
  private static synchronized void create_default_subscriber() {
    if (default_subscriber==null)
      default_subscriber = new Subscriber();
  }
  
  /**
   * Returns the default subscriber, whose settings are determined by the config
   */
  public static Subscriber subscriber() {
    if (default_subscriber==null)
      create_default_subscriber();
    return default_subscriber;
  }
  
  /**
   * Returns the default publisher, whose settings are determined by the config
   */
  public static Publisher publisher() {
    if (default_publisher==null)
      create_default_publisher();
    return default_publisher;
  }
  
  /**
   * Subscribes to the specified topic, registering the provided callback, using
   * the default subscriber.
   * @param topic the topic to subscribe to
   * @param callback the callback to register
   */
  public static void subscribe(String topic, Callback<?> callback) {
    subscriber().subscribe(topic, callback);
  }
  
  /**
   * Unsubscribes the provided callback from the specified topic
   * @param topic the topic to unsubscribe from
   * @param callback the callback to unsubscribe, if subscribed
   */
  public static void unsubscribe(String topic, Callback<?> callback) {
    subscriber().unsubscribe(topic, callback);
  }


  /**
   * Publishes to the specified topic and message using the default publisher
   * @param topic the topic to publish on
   * @param message the message to publish
   */
  public static void publish(String topic, Message message) {
    publisher().publish(topic, message);
  }
  

  
  /**
   * Subscribes to the specified topic, registering the provided callback, using
   * the default subscriber.
   * @param topic the topic to subscribe to
   * @param callback the callback to register
   */
  public static void subscribe(byte[] topic, Callback<?> callback) {
    subscriber().subscribe(topic, callback);
  }


  /**
   * Publishes to the specified topic and message using the default publisher
   * @param topic the topic to publish on
   * @param message the message to publish
   */
  public static void publish(byte[] topic, Message message) {
    publisher().publish(topic, message);
  }
  
  
  
}
