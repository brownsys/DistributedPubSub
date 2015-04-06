package edu.brown.cs.systems.pubsub.server.impl;

import java.nio.channels.SocketChannel;
import java.util.Set;

import com.google.common.collect.Sets;

class ServerConnectionManager {
  
  final PubSubServerImpl pubsub;
  final Set<ServerConnection> connected;
  
  ServerConnectionManager(PubSubServerImpl pubsub) {
    this.pubsub = pubsub;
    this.connected = Sets.newHashSet();
  }
  
  ServerConnection register(SocketChannel channel) {
    ServerConnection client = new ServerConnection(pubsub, channel);
    connected.add(client);
    return client;
  }
  
  void close(ServerConnection client) {
    pubsub.subscriptions.unsubscribeAll(client);
    pubsub.server.close(client.channel);
    connected.remove(client);
  }
  
  void closeAll() {
    pubsub.subscriptions.clear();
    for (ServerConnection client : connected) {
      pubsub.server.close(client.channel);
    }
    connected.clear();
  }

}
