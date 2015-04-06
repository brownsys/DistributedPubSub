package edu.brown.cs.systems.pubsub.client.impl;

import java.lang.reflect.ParameterizedType;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import edu.brown.cs.systems.pubsub.WireFormat;

public abstract class Callback<T extends Message> {

  private final Parser<T> parser;

  @SuppressWarnings("unchecked")
  public Callback() {
    Parser<T> parser = null;
    try {
      Class<?> cl = getClass();
      while (!Callback.class.equals(cl.getSuperclass())) {
        // case of multiple inheritance, we are trying to get the first
        // available generic info
        if (cl.getGenericSuperclass() instanceof ParameterizedType) {
          break;
        }
        cl = cl.getSuperclass();
      }
      Class<T> type = ((Class<T>) ((ParameterizedType) cl
          .getGenericSuperclass()).getActualTypeArguments()[0]);
      parser = (Parser<T>) type.getDeclaredField("PARSER").get(null);
    } catch (Exception e) {
      System.out.println("Error: callback creation failed");
      e.printStackTrace();
    }
    this.parser = parser;
  }

  protected abstract void OnMessage(T message);

  void OnMessage(byte[] serialized) {
    try {
      OnMessage(WireFormat.message(serialized, parser));
    } catch (InvalidProtocolBufferException e) {
      System.out.println("PubSub error deserializing message");
      e.printStackTrace();
    }
  }
}
