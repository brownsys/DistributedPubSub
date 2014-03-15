package edu.brown.cs.systems.pubsub;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.zeromq.ZMQ;

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
    registerCallback(topic, callback);
    socket.subscribe(topic.getBytes());
  }

  private static final Map<String, Collection<Callback<?>>> callbacks = new HashMap<String, Collection<Callback<?>>>();

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      byte[] envelope = socket.recv(0);
      byte[] bytes = socket.recv(0);
      String topic = new String(envelope);
      onMessage(topic, bytes);
    }
    socket.close();
  }

  private synchronized void registerCallback(String topic, Callback<?> callback) {
    Collection<Callback<?>> topic_callbacks = callbacks.get(topic);
    if (topic_callbacks == null)
      topic_callbacks = new ArrayList<Callback<?>>();
    topic_callbacks.add(callback);
    callbacks.put(topic, topic_callbacks);
  }

  private synchronized void onMessage(String topic, byte[] payload) {
    Collection<Callback<?>> tocall = callbacks.get(topic);
    if (tocall == null)
      return;
    for (Callback<?> callback : tocall) {
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
