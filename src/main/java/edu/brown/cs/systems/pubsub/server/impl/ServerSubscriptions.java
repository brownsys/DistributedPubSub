package edu.brown.cs.systems.pubsub.server.impl;

import java.util.Collection;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;

public class ServerSubscriptions {

  final PubSubServerImpl pubsub;
  final Multimap<ByteString, ServerConnection> topics = HashMultimap.create();
  final Multimap<ServerConnection, ByteString> clients = HashMultimap.create();

  public ServerSubscriptions(PubSubServerImpl pubsub) {
    this.pubsub = pubsub;
  }

  void unsubscribeAll(ServerConnection client) {
    for (ByteString topic : clients.removeAll(client)) {
      topics.remove(topic, client);
    }
  }

  void subscribe(ServerConnection client, ByteString topic) {
    topics.put(topic, client);
    clients.put(client, topic);
  }

  void unsubscribe(ServerConnection client, ByteString topic) {
    topics.remove(topic, client);
    clients.remove(client, topic);
  }

  void clear() {
    topics.clear();
    clients.clear();
  }

  Collection<ServerConnection> subscribers(ByteString topic) {
    return topics.get(topic);
  }

}
