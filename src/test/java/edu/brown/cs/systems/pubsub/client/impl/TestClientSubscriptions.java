package edu.brown.cs.systems.pubsub.client.impl;

import junit.framework.TestCase;

import org.junit.Test;

import com.google.protobuf.ByteString;

import edu.brown.cs.systems.pubsub.PubSubProtos.StringMessage;
import edu.brown.cs.systems.pubsub.WireFormat;

public class TestClientSubscriptions extends TestCase {
  
  private Callback<StringMessage> newCallback() {
    return new Callback<StringMessage>() {
      protected void OnMessage(StringMessage message) {
      }};
  }
  
  
  @Test
  public void testClientSubscriptions() {
    
    ClientSubscriptions subs = new ClientSubscriptions();
    
    Callback<StringMessage> cb1 = newCallback();
    Callback<StringMessage> cb2 = newCallback();

    ByteString topica = WireFormat.topic("topica");
    ByteString topicb = WireFormat.topic("topicb");
    
    assertEquals(0, subs.topics().size());
    for (int i = 0; i < 10; i++) {
      assertEquals(i, subs.callbacks(topica).length);
      subs.add(WireFormat.topic("topica"), cb1);
      assertEquals(i+1, subs.callbacks(topica).length);
      assertEquals(1, subs.topics().size());
    }
    
    assertEquals(1, subs.topics().size());
    for (int i = 0; i < 10; i++) {
      assertEquals(10, subs.callbacks(topica).length);
      assertEquals(i, subs.callbacks(topicb).length);
      subs.add(WireFormat.topic("topicb"), cb1);
      assertEquals(i+1, subs.callbacks(topicb).length);
      assertEquals(2, subs.topics().size());
    }
    
    for (int i = 10; i > 0; i--) {
      assertEquals(10, subs.callbacks(topica).length);
      assertEquals(i, subs.callbacks(topicb).length);
      assertEquals(2, subs.topics().size());
      subs.remove(WireFormat.topic("topicb"), cb1);
      assertEquals(i-1, subs.callbacks(topicb).length);      
    }
    assertEquals(1, subs.topics().size());
    
    for (int i = 10; i > 0; i--) {
      assertEquals(i, subs.callbacks(topica).length);
      assertEquals(1, subs.topics().size());
      subs.remove(WireFormat.topic("topica"), cb1);
      assertEquals(i-1, subs.callbacks(topica).length);      
    }
    assertEquals(0, subs.topics().size());
    
  }
  

}
