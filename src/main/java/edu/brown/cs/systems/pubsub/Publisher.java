//package edu.brown.cs.systems.pubsub;
//
//import java.util.Date;
//
//import org.zeromq.ZMQ;
//
//import com.google.protobuf.Message;
//
//public class Publisher {
//
//  /** The address that this publisher publishes to */
//  public final String address;
//  private final ZMQ.Context zmq;
//  private final ZMQ.Socket socket;
//
//  /**
//   * Creates a publisher that publishes to the default
//   * hostname and port, as specified in the application conf
//   */
//  public Publisher() {
//    this(Settings.SERVER_HOSTNAME, Settings.CLIENT_PUBLISH_PORT);
//  }
//
//  /**
//   * Create a publisher that publishes to a specific url
//   * @param hostname the hostname or ip address to publish to
//   * @param port the port to publish to
//   */
//  public Publisher(String hostname, int port) {
//    zmq = ZMQ.context(1);
//    socket = zmq.socket(ZMQ.PUB);
//    address = String.format("tcp://%s:%d", hostname, port);
//    socket.connect(address);
//    socket.setHWM(Settings.OUTGOING_MESSAGE_BUFFER_SIZE);
//    socket.setLinger(200);
//
//    // Sleep for 200ms to prevent slow joiners.
//    // Could be solved more elegantly but not for now.
//    try {
//      Thread.sleep(200);
//    } catch (InterruptedException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
//  }
//
//  /**
//   * Publishes a protocol buffers message on a topic
//   * @param topic the name of the topic to publish to
//   * @param message the protobuf message to publish
//   */
//  public void publish(String topic, Message message) {
//    publish(topic.getBytes(), message);
//  }
//
//  /**
//   * Publishes a protocol buffers message on a topic
//   * @param topic the byte representation of the topic
//   * @param message the protobuf message to publish
//   */
//  public void publish(byte[] topic, Message message) {
//    byte[] payload = message.toByteArray();
//    synchronized(socket) {
//      socket.send(topic, ZMQ.SNDMORE);
//      socket.send(payload, 0);
//    }
//  }
//
//  /**
//   * Close the publisher
//   */
//  public void close() {
//    synchronized(socket) {
//      socket.close();
//      zmq.close();
//    }
//  }
//
//  public static void main(String[] args) throws InterruptedException {
//    String server_hostname = Settings.SERVER_HOSTNAME;
//    int server_port = Settings.CLIENT_PUBLISH_PORT;
//    if (args.length > 1)
//      server_hostname = args[0];
//    if (args.length > 2)
//      server_port = Integer.parseInt(args[1]);
//    Publisher publisher = new Publisher(server_hostname, server_port);
//    String topic = "current_date";
//    System.out.println("Publishing to topic " + topic);
//    while (true) {
//      String message = new Date().toString();
//      System.out.println("Sent: " + message);
//      publisher.publish(topic, PubSubProtos.StringMessage.newBuilder().setMessage(message).build());
//      Thread.sleep(1000);
//    }
//  }
//}
