package edu.brown.cs.systems.pubsub;

import com.google.protobuf.Message;

/**
 * Legacy subscriber class. Still here because it defines the callback class
 */
@Deprecated
public interface Subscriber {

  /** Legacy callback class. Extends edu.brown.cs.systems.pubsub.Callback */
  @Deprecated
  public abstract class Callback<T extends Message> extends
      edu.brown.cs.systems.pubsub.client.impl.Callback<T> {
  }

}
