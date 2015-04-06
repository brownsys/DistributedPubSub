package edu.brown.cs.systems.pubsub.client.impl;

import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

import com.google.common.collect.Lists;

import edu.brown.cs.systems.pubsub.PubSubProtos.StringMessage;
import edu.brown.cs.systems.pubsub.Subscriber;
import edu.brown.cs.systems.pubsub.WireFormat;

public class TestCallback extends TestCase {

  @Test
  public void testCallback() {
    List<String> strs = Lists.newArrayList("asdf", "duso", "",
        "asodfokjwefowifdfs", "ASDF:!@RL<FSDFsdfolasdjfp2kl1;r3kmj");

    for (final String str : strs) {
      Callback<StringMessage> cb1 = new Subscriber.Callback<StringMessage>() {

        @Override
        protected void OnMessage(StringMessage message) {
          assertEquals(str, message.getMessage());
        }
      };

      Callback<StringMessage> cb2 = new Callback<StringMessage>() {

        @Override
        protected void OnMessage(StringMessage message) {
          assertEquals(str, message.getMessage());
        }
      };

      cb1.OnMessage(WireFormat.publish(WireFormat.topic("topic"), StringMessage
          .newBuilder().setMessage(str).build()));
      cb2.OnMessage(WireFormat.publish(WireFormat.topic("topic"), StringMessage
          .newBuilder().setMessage(str).build()));
    }
  }

}
