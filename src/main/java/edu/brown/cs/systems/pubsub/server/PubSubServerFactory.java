package edu.brown.cs.systems.pubsub.server;

import java.io.IOException;

import edu.brown.cs.systems.pubsub.PubSubConfig;
import edu.brown.cs.systems.pubsub.server.impl.PubSubServerImpl;

public class PubSubServerFactory {

  public static PubSubServer create() throws IOException {
    return create(PubSubConfig.Server.bindto(), PubSubConfig.Server.port());
  }

  public static PubSubServer create(String bindto, int port) throws IOException {
    return create(bindto, port, PubSubConfig.Server.receiveBufferSize(),
        PubSubConfig.Server.sendBufferSize());
  }

  public static PubSubServer create(String bindto, int port,
      int receiveBufferSize, int sendBufferSize) throws IOException {
    return new PubSubServerImpl(bindto, port, receiveBufferSize, sendBufferSize);
  }

}
