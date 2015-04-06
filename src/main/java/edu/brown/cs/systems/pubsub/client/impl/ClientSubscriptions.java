package edu.brown.cs.systems.pubsub.client.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.protobuf.ByteString;

class ClientSubscriptions {

  private final Map<ByteString, Multiset<Callback<?>>> callbacks = Maps.newHashMap();

  /**
   * @return true if this is the first callback on the topic, false otherwise
   */
  synchronized boolean add(ByteString topic, Callback<?> callback) {
    boolean isFirstCallback = false;
    Multiset<Callback<?>> current = callbacks.get(topic);
    if (current == null) {
      current = HashMultiset.create();
      callbacks.put(topic, current);
      isFirstCallback = true;
    }
    current.add(callback);
    return isFirstCallback;
  }

  /**
   * @return true if there are no further callbacks on the topic
   */
  synchronized boolean remove(ByteString topic, Callback<?> callback) {
    Multiset<Callback<?>> current = callbacks.get(topic);
    if (current == null)
      return false;
    if (current.remove(callback) && current.isEmpty()) {
      callbacks.remove(topic);
      return true;
    }
    return false;
  }

  /**
   * @return true if there are no further callbacks on the topic
   */
  synchronized boolean removeAll(ByteString topic, Callback<?> callback) {
    Multiset<Callback<?>> current = callbacks.get(topic);
    if (current == null)
      return false;
    if (current.remove(callback, current.count(callback)) > 0 && current.isEmpty()) {
      callbacks.remove(topic);
      return true;
    }
    return false;
  }

  synchronized void removeAll(ByteString topic) {
    callbacks.remove(topic);
  }

  synchronized void clear() {
    callbacks.clear();
  }

  synchronized void invoke(ByteString topic, byte[] message) {
    for (Callback<?> callback : callbacks.get(topic)) {
      callback.OnMessage(message);
    }
  }

  synchronized Set<ByteString> topics() {
    return new HashSet<ByteString>(callbacks.keySet());
  }
  
  synchronized Multiset<Callback<?>> callbacks(ByteString topic) {
    return HashMultiset.create(callbacks.get(topic));
  }

}
