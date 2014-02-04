package edu.brown.cs.systems.pubsub;

import akka.actor.ActorSystem;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class Settings {
  
  private static final int[] SEED_PORTS = new int[] { 2551, 2552, 2553, 2554, 2555, 2556, 2557, 2558, 2559 };

  public static ActorSystem getActorSystem() {
    for (int port : SEED_PORTS) {
      try {
        Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port="+port).withFallback(ConfigFactory.load());
        ActorSystem system = ActorSystem.create("PubSub", config);
        System.out.println("Created PubSub ActorSystem on port " + port);
        return system;
      } catch (Exception e) {
        System.out.println("Unable to create PubSub actor system on port " + port);
      }
    }
    ActorSystem system = ActorSystem.create("PubSub", ConfigFactory.load());
    System.out.println("Created PubSub ActorSystem on random port");
    return system;
  }
  
}
