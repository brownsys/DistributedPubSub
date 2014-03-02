package edu.brown.cs.systems.pubsub;

import org.zeromq.ZMQ;

public class PubSubServer {

    public static void initialize() {
        server_msg_proxy_thread.setDaemon(true);
        server_msg_proxy_thread.start();
    }
    
    private static final Thread server_msg_proxy_thread = new Thread() {
        @Override
        public void run() {
            // Prepare our context, publisher and subscribers
            ZMQ.Context context = ZMQ.context(1);
            
            ZMQ.Socket publisher = context.socket(ZMQ.PUB);
            publisher.setHWM(Settings.OUTGOING_MESSAGE_BUFFER_SIZE);
            
            ZMQ.Socket subscriber = context.socket(ZMQ.SUB);

            publisher.bind(String.format("tcp://0.0.0.0:%d", Settings.CLIENT_SUBSCRIBE_PORT));
            subscriber.bind(String.format("tcp://0.0.0.0:%d", Settings.CLIENT_PUBLISH_PORT));
            subscriber.subscribe("".getBytes());
            
            System.out.println(String.format("PubSub server listening for messages at 0.0.0.0:%d", Settings.CLIENT_PUBLISH_PORT));
            System.out.println(String.format("PubSub server accepting subscriptions at 0.0.0.0:%d", Settings.CLIENT_SUBSCRIBE_PORT));
            while (!Thread.currentThread().isInterrupted()) {
                byte[] envelope = subscriber.recv(0);
                byte[] bytes = subscriber.recv(0);
                publisher.send(envelope, ZMQ.SNDMORE);
                publisher.send(bytes, 0);
            }
            
            publisher.close();
            subscriber.close();
        }
    };
    
    public static void main(String[] args) throws InterruptedException {
        initialize();
        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(1000);
        }
    }

}
