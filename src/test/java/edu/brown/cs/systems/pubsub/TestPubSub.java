package edu.brown.cs.systems.pubsub;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import edu.brown.cs.systems.pubsub.PubSubProtos.StringMessage;
import edu.brown.cs.systems.pubsub.Subscriber.Callback;
import edu.brown.cs.systems.pubsub.client.PubSubClient;
import edu.brown.cs.systems.pubsub.client.PubSubClientFactory;
import edu.brown.cs.systems.pubsub.server.PubSubServer;
import edu.brown.cs.systems.pubsub.server.PubSubServerFactory;

public class TestPubSub {

  
  
  private static final class TestPubSubCallback extends Callback<PubSubProtos.StringMessage> {
    
    private static final int wait_time = 1000;
    
    public volatile String msg = null;
    private Semaphore semaphore = new Semaphore(0);
    
    public void awaitMessage(String msg) throws InterruptedException {
      assertTrue(semaphore.tryAcquire(wait_time, TimeUnit.MILLISECONDS));
      assertTrue(msg.equals(this.msg));
      reset();
    }
    
    public void awaitNoMessage(String msg) throws InterruptedException {
      assertFalse(semaphore.tryAcquire(wait_time, TimeUnit.MILLISECONDS));
      assertFalse(msg.equals(this.msg));
      assertNull(this.msg);
      reset();      
    }
    
    public void reset() {
      semaphore.drainPermits();
      msg = null;
    }

    @Override
    protected void OnMessage(StringMessage message) {
      msg = message.getMessage();
      semaphore.release();
    }
    
  }
  
  @Test
  public void testBPubSub() throws InterruptedException, IOException {
    String topic = "test";
    
    PubSubServer server = PubSubServerFactory.create();
    PubSubClient client = PubSubClientFactory.create();
    
    // Subscribe to test topic
    TestPubSubCallback cb = new TestPubSubCallback();
    client.subscribe(topic, cb);
    Thread.sleep(1000);
    
    // Publish a message
    System.out.println("Publisher publishing " + "hello" + " on topic " + topic);
    client.publish(topic, PubSubProtos.StringMessage.newBuilder().setMessage("hello").build());
    cb.awaitMessage("hello");
    
    // Publish a message
    System.out.println("Publisher publishing " + "hello" + " on topic " + "badtest");
    client.publish("badtest", PubSubProtos.StringMessage.newBuilder().setMessage("hello").build());
    cb.awaitNoMessage("hello");
    
    // Publish a message
    System.out.println("Publisher publishing " + "hello2" + " on topic " + topic);
    client.publish(topic, PubSubProtos.StringMessage.newBuilder().setMessage("hello2").build());
    cb.awaitMessage("hello2");
    
    // Unsubscribe
    client.unsubscribe(topic, cb);
    Thread.sleep(1000);
    
    // Publish a message
    System.out.println("Publisher publishing " + "hello2" + " on topic " + topic);
    client.publish(topic, PubSubProtos.StringMessage.newBuilder().setMessage("hello2").build());
    cb.awaitNoMessage("hello2");
    
    // Close the server and client
    server.shutdown();
    client.shutdown();
    server.awaitTermination();
    client.awaitTermination();
  }


}
