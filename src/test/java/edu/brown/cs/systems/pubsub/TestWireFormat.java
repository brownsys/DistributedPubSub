package edu.brown.cs.systems.pubsub;

import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import com.google.common.collect.Lists;

public class TestWireFormat extends TestCase {
  
  @Test
  public void testTopicEquality() {
    
    List<String> strs = Lists.newArrayList("a", "A", "aa", "aA", "Aa", "AA", "topic", "asdf0234K@#RL");

    for (String str : strs) {
      for (String str2 : strs) {
        assertEquals(WireFormat.topic(str), WireFormat.topic(str));
        if (!str.equals(str2)) {
          assertFalse(WireFormat.topic(str).equals(WireFormat.topic(str2)));
        } else {
          assertEquals(WireFormat.topic(str), WireFormat.topic(str2));          
        }
      }
    }
  }

}
