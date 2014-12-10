package edu.brown.cs.systems.pubsub;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;

import org.zeromq.ZMQ;

import com.google.common.collect.HashMultimap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class Subscriber extends Thread {

  public static abstract class Callback<T extends Message> {

    private final Parser<T> parser;

    public Callback() {
      Parser<T> parser = null;
      try {
        Class<?> cl = getClass();
        while (!Callback.class.equals(cl.getSuperclass())) {
            // case of multiple inheritance, we are trying to get the first available generic info
            if (cl.getGenericSuperclass() instanceof ParameterizedType) {
                break;
            }
            cl = cl.getSuperclass();
        }
        Class<T> type = ((Class<T>) ((ParameterizedType) cl.getGenericSuperclass()).getActualTypeArguments()[0]);
        parser = (Parser<T>) type.getDeclaredField("PARSER").get(null);
      } catch(Exception e) {
        System.out.println("Error: callback creation failed");
        e.printStackTrace();
      }
      this.parser = parser;
    }

    protected abstract void OnMessage(T message);

    private void OnMessage(byte[] payload) {
      try {
        OnMessage(parser.parseFrom(payload));
      } catch (InvalidProtocolBufferException e) {
        System.out.println("PubSub error deserializing message");
        e.printStackTrace();
      }
    }
  }

  public final String address;
  private final ZMQ.Socket socket;
  
  /**
   * Creates a new subscriber that connects to the pub sub server at
   * the hostname and port specified by the application conf.  The
   * subscriber thread is started by the constructor and will call
   * any registered callbacks.
   */
  public Subscriber() {
    this(Settings.SERVER_HOSTNAME, Settings.CLIENT_SUBSCRIBE_PORT);
  }

  /**
   * Creates a new subscriber that connects to the pub sub server at the
   * specified hostname and port. The subscriber thread is started by the
   * constructor, and will call any registered callbacks
   */
  public Subscriber(String hostname, int port) {
    socket = PubSub.context.socket(ZMQ.SUB);
    address = String.format("tcp://%s:%d", hostname, port);
    socket.connect(address);
    setDaemon(true);
    start();
    System.out.println("Subscriber subscribing to " + address);
  }

  /**
   * Subscribes to the specified topic, and registers the provided callback to
   * be called for each new message on the topic
   * @param topic the name of the topic to subscribe to
   * @param callback the callback to call when messages are received
   */
  public void subscribe(String topic, Callback<?> callback) {
    this.subscribe(topic.getBytes(), callback);
  }

  /**
   * Subscribes to the specified topic, and registers the provided callback to
   * be called for each new message on the topic
   * @param topic the name of the topic to subscribe to
   * @param callback the callback to call when messages are received
   */
  public void subscribe(byte[] topic, Callback<?> callback) {
    synchronized(this) {
      callbacks.put(Arrays.hashCode(topic), callback);
      socket.subscribe(topic);
    }
  }
  
  /**
   * Unsubscribes from the specified topic
   * @param topic
   * @param callback
   */
  public void unsubscribe(String topic, Callback<?> callback) {
    this.unsubscribe(topic.getBytes(), callback);
  }
  
  public void unsubscribe(byte[] topic, Callback<?> callback) {
    int topichash = Arrays.hashCode(topic);
    synchronized(this) {
      callbacks.remove(topichash, callback);
      if (callbacks.get(topichash).isEmpty()) {
        socket.unsubscribe(topic);
      }
    }
  }
  
  public void close() {
    this.interrupt();
  }
  
  private static final HashMultimap<Integer, Callback<?>> callbacks = HashMultimap.create();

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        byte[] topic = socket.recv(0);
        byte[] bytes = socket.recv(0);
        onMessage(topic, bytes);
      } catch (Exception e) {
        // squelch;
      }
    }
    socket.close();
    System.out.println("Subscriber closed.");
  }

  private synchronized void onMessage(byte[] topic, byte[] payload) {
    for (Callback<?> callback : callbacks.get(Arrays.hashCode(topic))) {
      try {
        callback.OnMessage(payload);
      } catch (Exception e) {
        System.out.println("Exception calling callback for PubSub message");
        e.printStackTrace();
      }
    }
  }
  
  public static void main(String[] args) throws InterruptedException {
    String server_hostname = Settings.SERVER_HOSTNAME;
    int server_port = Settings.CLIENT_SUBSCRIBE_PORT;
    if (args.length > 1)
      server_hostname = args[0];
    if (args.length > 2)
      server_port = Integer.parseInt(args[1]);
    Subscriber subscriber = new Subscriber(server_hostname, server_port);
    String topic = "current_date";
    System.out.println("Subscribing to topic " + topic);
    subscriber.subscribe(topic, new Callback<PubSubProtos.StringMessage>() {
      @Override
      protected void OnMessage(PubSubProtos.StringMessage m) {
        System.out.println("Received: " + m.getMessage());
      }
    });
    synchronized (Thread.currentThread()) {
      Thread.currentThread().wait();
    }
  }

}
