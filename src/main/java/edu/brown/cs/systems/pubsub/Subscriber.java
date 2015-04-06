package edu.brown.cs.systems.pubsub;

import com.google.protobuf.Message;

/**
 * Legacy subscriber class. Still here because it defines the callback class
 */
public interface Subscriber {

  /** Legacy callback class. Extends edu.brown.cs.systems.pubsub.Callback */
  public abstract class Callback<T extends Message> extends
      edu.brown.cs.systems.pubsub.client.impl.Callback<T> {
  }

}
