package edu.brown.cs.systems.pubsub.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import edu.brown.cs.systems.pubsub.client.impl.Callback;

public interface PubSubClient {

  /**
   * Close the client, stopping any threads associated with the client
   */
  public void shutdown();

  /**
   * Wait for the client thread(s) to terminate. This call will block until
   * interrupted or the client terminates. Clients should call shutdown first to
   * shut down the client, if desired.
   * 
   * @throws InterruptedException
   */
  public void awaitTermination() throws InterruptedException;

  /**
   * Publishes a protocol buffers message on a topic
   * 
   * @param topic
   *          the name of the topic to publish to
   * @param message
   *          the protobuf message to publish
   * @return true if the message was successfully published, false otherwise
   */
  public boolean publish(String topic, Message message);

  /**
   * Publishes a protocol buffers message on a topic
   * 
   * @param topic
   *          the byte representation of the topic
   * @param message
   *          the protobuf message to publish
   * @return true if the message was successfully published, false otherwise
   */
  public boolean publish(byte[] topic, Message message);

  /**
   * Publishes a protocol buffers message on a topic
   * 
   * @param topic
   *          the byte representation of the topic
   * @param message
   *          the protobuf message to publish
   * @return true if the message was successfully published, false otherwise
   */
  public boolean publish(ByteString topic, Message message);

  /**
   * Subscribes to the specified topic, and registers the provided callback to
   * be called for each new message on the topic
   * 
   * @param topic
   *          the name of the topic to subscribe to
   * @param callback
   *          the callback to call when messages are received
   */
  public void subscribe(String topic, Callback<?> callback);

  /**
   * Subscribes to the specified topic, and registers the provided callback to
   * be called for each new message on the topic
   * 
   * @param topic
   *          the name of the topic to subscribe to
   * @param callback
   *          the callback to call when messages are received
   */
  public void subscribe(byte[] topic, Callback<?> callback);

  /**
   * Subscribes to the specified topic, and registers the provided callback to
   * be called for each new message on the topic
   * 
   * @param topic
   *          the name of the topic to subscribe to
   * @param callback
   *          the callback to call when messages are received
   */
  public void subscribe(ByteString topic, Callback<?> callback);

  /**
   * Unsubscribes from the specified topic
   * 
   * @param topic
   *          the name of the topic to unsubscribe from
   * @param callback
   *          the callback to remove
   */
  public void unsubscribe(String topic, Callback<?> callback);

  /**
   * Unsubscribes from the specified topic
   * 
   * @param topic
   *          the name of the topic to unsubscribe from
   * @param callback
   *          the callback to remove
   */
  public void unsubscribe(byte[] topic, Callback<?> callback);

  /**
   * Unsubscribes from the specified topic
   * 
   * @param topic
   *          the name of the topic to unsubscribe from
   * @param callback
   *          the callback to remove
   */
  public void unsubscribe(ByteString topic, Callback<?> callback);

}
