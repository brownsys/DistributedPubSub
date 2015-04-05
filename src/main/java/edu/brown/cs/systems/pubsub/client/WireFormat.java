package edu.brown.cs.systems.pubsub.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import edu.brown.cs.systems.pubsub.PubSubProtos.Header;

class WireFormat {
  
  static byte[] publish(String topic, Message message) {
    return publish(ByteString.copyFromUtf8(topic), message);
  }
  
  static byte[] publish(ByteString topic, Message message) {
    Header header = Header.newBuilder()
        .setMessageType(Header.MessageType.PUBLISH)
        .setTopic(topic)
        .build();

    return serialize(header, message);
  }
  
  static byte[] subscribe(String topic) {
    return subscribe(ByteString.copyFromUtf8(topic));
  }
  
  static byte[] subscribe(ByteString topic) {
    Header header = Header.newBuilder()
        .setMessageType(Header.MessageType.SUBSCRIBE)
        .setTopic(topic)
        .build();
    
    return serialize(header);
  }
  
  static byte[] unsubscribe(String topic) {
    return unsubscribe(ByteString.copyFromUtf8(topic));
  }
  
  static byte[] unsubscribe(ByteString topic) {
    Header header = Header.newBuilder()
        .setMessageType(Header.MessageType.UNSUBSCRIBE)
        .setTopic(topic)
        .build();
    
    return serialize(header);
  }
  
  static byte[] serialize(Header header, Message message) {
    // Calculate the size of the outgoing message
    int headerlen = header.getSerializedSize();
    int messagelen = message.getSerializedSize();
    int len = 4 // header size
        + headerlen // header
        + messagelen; // payload

    // Serialize the message, abort if something goes wrong
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.putInt(headerlen);
    CodedOutputStream cos = CodedOutputStream.newInstance(buf.array(), buf.position(), buf.remaining());
    try {
      header.writeTo(cos);
      message.writeTo(cos);
    } catch (IOException e) {
      return null;
    }
    return buf.array();
  }
  
  static byte[] serialize(Header header) {
    // Calculate the size of the outgoing message
    int headerlen = header.getSerializedSize();
    int len = 4 // header size
        + headerlen; // header

    // Serialize the message, abort if something goes wrong
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.putInt(headerlen);
    CodedOutputStream cos = CodedOutputStream.newInstance(buf.array(), buf.position(), buf.remaining());
    try {
      header.writeTo(cos);
    } catch (IOException e) {
      return null;
    }
    return buf.array();
  }
  
  static Header header(byte[] serialized) throws IOException {
    ByteBuffer buf = ByteBuffer.wrap(serialized);
    int headerSize = buf.getInt();
    return Header.parseFrom(new ByteArrayInputStream(serialized,
        buf.position(), headerSize));
  }
  
  static <T extends Message> T message(byte[] serialized, Parser<T> parser) throws InvalidProtocolBufferException {
    ByteBuffer buf = ByteBuffer.wrap(serialized);
    int messageBegin = buf.getInt() + buf.position();
    int messageLen = serialized.length - messageBegin;
    InputStream is = new ByteArrayInputStream(serialized, messageBegin, messageLen);
    return (T) parser.parseFrom(is);
  }

}
