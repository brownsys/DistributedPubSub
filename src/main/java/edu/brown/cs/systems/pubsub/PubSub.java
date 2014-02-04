package edu.brown.cs.systems.pubsub;

import java.io.Serializable;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.contrib.pattern.DistributedPubSubExtension;
import akka.contrib.pattern.DistributedPubSubMediator;

public class PubSub {
  
  public interface Message extends Serializable {}

  public static abstract class Callback {
    protected abstract void OnMessage(Message m);
  }
  
  private static ActorSystem system = Settings.getActorSystem();
  private static ActorRef mediator = DistributedPubSubExtension.get(system).mediator();
  
  public static void publish(String topic, Message message) {
    mediator.tell(new DistributedPubSubMediator.Publish(topic, message), null);
  }
  
  public static void subscribe(String topic, Callback callback) {
    system.actorOf(Props.create(Subscriber.class, topic, callback), "Subscriber");
  }
  
  private static class Subscriber extends UntypedActor {
    private final Callback callback;
    
    public Subscriber(String topic, Callback callback) {
      this.callback = callback;
      mediator.tell(new DistributedPubSubMediator.Subscribe(topic, getSelf()), getSelf());
    }
    
    @Override
    public void onReceive(Object msg) {
      if (msg instanceof Message)
        callback.OnMessage((Message) msg);
      else
        unhandled(msg);
    }
  }

}
