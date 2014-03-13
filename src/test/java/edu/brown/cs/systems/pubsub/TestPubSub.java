package edu.brown.cs.systems.pubsub;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.brown.cs.systems.pubsub.PubSubProtos.StringMessage;
import edu.brown.cs.systems.pubsub.Subscriber.Callback;

public class TestPubSub {

  /**
   * Tests to see whether the ZMQ bindings work
   */
  @Test
  public void testAZMQ() {
    try {
      if (PubSub.context != null) {
        System.out.println("ZMQ Binding successful");
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Unable to bind ZMQ");
    }
  }
  
  private static final class TestPubSubCallback extends Callback<PubSubProtos.StringMessage> {

    String rcvd = null;
    
    @Override
    protected void OnMessage(StringMessage message) {
      rcvd = message.getMessage();
      System.out.println("Subscriber received " + rcvd);
    }
    
  }
  
  @Test
  public void testBPubSub() throws InterruptedException {
    Server server = new Server("127.0.0.1", Settings.CLIENT_SUBSCRIBE_PORT, Settings.CLIENT_PUBLISH_PORT);
    server.start();
    Publisher publisher = new Publisher("127.0.0.1", Settings.CLIENT_PUBLISH_PORT);
    Subscriber subscriber = new Subscriber("127.0.0.1", Settings.CLIENT_SUBSCRIBE_PORT);
    TestPubSubCallback cb = new TestPubSubCallback();
    subscriber.subscribe("test", cb);
    Thread.sleep(1000);
    System.out.println("Publisher publishing " + "hello" + " on topic " + "test");
    publisher.publish("test", PubSubProtos.StringMessage.newBuilder().setMessage("hello").build());
    Thread.sleep(1000);
    assertEquals("hello", cb.rcvd);
    cb.rcvd = null;
    System.out.println("Publisher publishing " + "hello" + " on topic " + "badtest");
    publisher.publish("badtest", PubSubProtos.StringMessage.newBuilder().setMessage("hello").build());
    Thread.sleep(1000);
    assertEquals(null, cb.rcvd);
  }


}
