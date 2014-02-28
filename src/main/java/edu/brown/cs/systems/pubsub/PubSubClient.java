package edu.brown.cs.systems.pubsub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import org.zeromq.ZMQ;

import edu.brown.cs.systems.pubsub.PubSub.Callback;
import edu.brown.cs.systems.pubsub.PubSub.Message;

public class PubSubClient {

    private static ZMQ.Context context = null;
    private static ZMQ.Socket publisher = null;
    private static ZMQ.Socket subscriber = null;
    
    private static final synchronized void initializeZMQ() {
        if (context==null)
            context = ZMQ.context(1);
    }
    
    private static final synchronized void initializePublisher() {
        initializeZMQ();
        if (publisher==null) {
            publisher = context.socket(ZMQ.PUB);
            publisher.connect(String.format("tcp://%s:%d", Settings.SERVER_HOSTNAME, Settings.CLIENT_PUBLISH_PORT));
            publisher.setHWM(Settings.OUTGOING_MESSAGE_BUFFER_SIZE);
        }
    }
    
    private static final synchronized void initializeSubscriber() {
        initializeZMQ();
        if (subscriber==null) {
            subscriber = context.socket(ZMQ.SUB);
            subscriber.connect(String.format("tcp://%s:%d", Settings.SERVER_HOSTNAME, Settings.CLIENT_SUBSCRIBE_PORT));
            subscribe_thread.setDaemon(true);
            subscribe_thread.start();
        }
    }

    public static void publish(String topic, Message message) {
        if (publisher==null)
            initializePublisher();
        try {
            byte[] envelope = topic.getBytes();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos);
            os.writeObject(message);
            byte[] msgbytes = bos.toByteArray();
            synchronized (publisher) {
                publisher.send(envelope, ZMQ.SNDMORE);
                publisher.send(msgbytes, 0);
            }
        } catch (IOException e) {
            System.out.println("IOException trying to send message");
            e.printStackTrace();
        }
    }
    
    private static final Map<String, Collection<Callback>> callbacks = new HashMap<String, Collection<Callback>>();
    
    public static void subscribe(String topic, Callback callback) {
        if (subscriber==null)
            initializeSubscriber();
        synchronized (callbacks) {
            if (!callbacks.containsKey(topic))
                callbacks.put(topic, new HashSet<Callback>());
            callbacks.get(topic).add(callback);
        }
        subscriber.subscribe(topic.getBytes());
    }
    
    private static final Thread subscribe_thread = new Thread() {
        @Override
        public void run() {            
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    byte[] envelope = subscriber.recv(0);
                    byte[] bytes = subscriber.recv(0);
                    String topic = new String(envelope);
                    ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(bytes));
                    Message m = (Message) oi.readObject();
                    synchronized(callbacks) {
                        Collection<Callback> tocall = callbacks.get(topic);
                        for (Callback c : tocall) {
                            try {
                                c.OnMessage(m);
                            } catch (Exception e) {
                                System.out.println("Exception calling callback for message: " + m);
                            }
                        }
                    }
                } catch (ClassNotFoundException e) {
                    System.out.println("Exception deserializing message");
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.println("Exception deserializing message");
                    e.printStackTrace();
                }
            }
            subscriber.close();
        }
    };
    
    public static void main(String[] args) {
        try {
            if (new Random().nextBoolean())
//            if (true)
                subscriber();
            else
                publisher();
        } catch (InterruptedException e) {
            
        }
    }
    
    public static class TestMessage implements Message {
        public String str = "";
        public TestMessage(String s) { str = s; }
        @Override
        public String toString() { return str; }
    }
    
    public static void subscriber() throws InterruptedException {
        System.out.println("Subscriber");
        PubSubClient.subscribe("testtopic", new Callback() {

            @Override
            protected void OnMessage(Message m) {
                System.out.println("Received message " + m);
            }});
        synchronized(Thread.currentThread()) {
            Thread.currentThread().wait();
        }
    }
    
    public static void publisher() throws InterruptedException {
        System.out.println("Publisher");
        while (true) {
            System.out.println("sending");
            PubSubClient.publish("testtopic", new TestMessage("boshank"));
            PubSubClient.publish("badtesttopic", new TestMessage("alicious"));
            Thread.sleep(1000);
        }
    }
    
}
