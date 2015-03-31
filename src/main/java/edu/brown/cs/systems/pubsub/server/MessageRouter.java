package edu.brown.cs.systems.pubsub.server;

import java.nio.ByteBuffer;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import edu.brown.cs.systems.pubsub.PubSubConfig;

/**
 * Routes incoming messages based on subscription
 */
public class MessageRouter {
	
	private final Multimap<String, Client> subscriptions = HashMultimap.create(); 
	private final Multimap<Client, String> client_subscriptions = HashMultimap.create();
	
	/** Subscribe a client to a particular topic */
	public void subscribe(String topic, Client client) {
		subscriptions.put(topic, client);
		client_subscriptions.put(client, topic);
	}
	
	/** Unsubscribe a client from a particular topic */
	public void unsubscribe(String topic, Client client) {
		subscriptions.remove(topic, client);
		client_subscriptions.remove(client, topic);
	}
	
	/** Unsubscribe the client from all topics */
	public void unsubscribeAll(Client client) {
		for (String topic : client_subscriptions.removeAll(client)) {
			subscriptions.remove(topic, client);
		}
	}
	
	public void route(Client from, byte[] message) {
		// First, get the topic
		ByteBuffer buf = ByteBuffer.wrap(message);
		int topicLength = buf.getInt();
		byte[] topicBytes = new byte[topicLength];
		buf.get(topicBytes);
		String topic = new String(topicBytes);
		
		// Handle subscribe / unsubscribe
		if (topic.equals(PubSubConfig.Server.subscribetopic())) {
			// Get the subscription topic
			byte[] subscriptionBytes = new byte[buf.remaining()];
			buf.get(subscriptionBytes);
			String subscriptionTopic = new String(subscriptionBytes);
			
			// Add the subscription
			subscribe(subscriptionTopic, from);
			
		} else if (topic.equals(PubSubConfig.Server.unsubscribetopic())) {
			// Get the subscription topic
			byte[] subscriptionBytes = new byte[buf.remaining()];
			buf.get(subscriptionBytes);
			String subscriptionTopic = new String(subscriptionBytes);
			
			// Remove the subscription
			unsubscribe(subscriptionTopic, from);
		}
		
		// Foward the message
		for (Client c : subscriptions.get(topic)) {
			c.out(message);
		}
	}

}
