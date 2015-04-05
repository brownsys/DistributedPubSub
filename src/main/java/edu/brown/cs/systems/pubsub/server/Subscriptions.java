package edu.brown.cs.systems.pubsub.server;

import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;

public class Subscriptions {

  final PubSubServer pubsub;
  final Multimap<ByteString, ConnectedClient> topics = HashMultimap.create();
  final Multimap<ConnectedClient, ByteString> clients = HashMultimap.create();

  public Subscriptions(PubSubServer pubsub) {
    this.pubsub = pubsub;
  }

  void unsubscribeAll(ConnectedClient client) {
    for (ByteString topic : clients.removeAll(client)) {
      topics.remove(topic, client);
    }
  }

  void subscribe(ConnectedClient client, ByteString topic) {
    topics.put(topic, client);
    clients.put(client, topic);
  }

  void unsubscribe(ConnectedClient client, ByteString topic) {
    topics.remove(topic, client);
    clients.remove(client, topic);
  }

  void clear() {
    topics.clear();
    clients.clear();
  }

  Collection<ConnectedClient> subscribers(ByteString topic) {
    return topics.get(topic);
  }

}
