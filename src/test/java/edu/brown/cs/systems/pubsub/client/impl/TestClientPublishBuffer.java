package edu.brown.cs.systems.pubsub.client.impl;

import junit.framework.TestCase;

import org.junit.Test;

public class TestClientPublishBuffer extends TestCase {
  
  @Test
  public void testCapacityCheck() {
    ClientPublishBuffer buf = new ClientPublishBuffer(0);
    
    assertTrue(buf.add(new byte[0]));
    assertFalse(buf.add(new byte[1]));
    assertTrue(buf.add(new byte[0]));
    assertFalse(buf.add(new byte[1]));
    

    buf = new ClientPublishBuffer(1024*1024);
    
    for (int i = 0; i < 1024; i++) {
      assertTrue(buf.add(new byte[1024]));
    }
    assertFalse(buf.add(new byte[1024]));
    buf.completed();
    assertTrue(buf.add(new byte[1024]));
    assertFalse(buf.add(new byte[1024]));
    
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < i; j++) {
        buf.completed();
      }
      for (int j = 0; j < i; j++) {
        assertTrue(buf.add(new byte[1024]));
      }
      assertFalse(buf.add(new byte[1024]));
    }
  }
  
  @Test
  public void testPeek() {
    ClientPublishBuffer buf = new ClientPublishBuffer(1024*1024);
    
    byte[] msg = new byte[1024];
    
    buf.add(msg);
    
    assertEquals(msg, buf.current());
    assertTrue(buf.hasMore());
    buf.completed();
    assertEquals(null, buf.current());
    assertFalse(buf.hasMore());
    
    byte[] a = new byte[1024];
    byte[] b = new byte[1024];
    
    buf.add(a);
    buf.add(b);
    
    assertEquals(a, buf.current());
    assertTrue(buf.hasMore());
    buf.completed();
    assertEquals(b, buf.current());
    assertTrue(buf.hasMore());
    buf.completed();
    assertEquals(null, buf.current());
    assertFalse(buf.hasMore());
  }

}
