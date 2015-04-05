package edu.brown.cs.systems.pubsub.server;

import java.nio.channels.SocketChannel;
import java.util.Set;

import com.google.common.collect.Sets;

class Connections {
  
  final PubSubServer pubsub;
  final Set<ConnectedClient> connected;
  
  Connections(PubSubServer pubsub) {
    this.pubsub = pubsub;
    this.connected = Sets.newHashSet();
  }
  
  ConnectedClient register(SocketChannel channel) {
    ConnectedClient client = new ConnectedClient(pubsub, channel);
    connected.add(client);
    return client;
  }
  
  void close(ConnectedClient client) {
    pubsub.subscriptions.unsubscribeAll(client);
    pubsub.server.close(client.channel);
    connected.remove(client);
  }
  
  void closeAll() {
    pubsub.subscriptions.clear();
    for (ConnectedClient client : connected) {
      pubsub.server.close(client.channel);
    }
    connected.clear();
  }

}
