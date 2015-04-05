package edu.brown.cs.systems.pubsub.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Queues;

class PublishBuffer {

  private final int maxPendingBytes;
  private final BlockingQueue<byte[]> q = Queues.newLinkedBlockingQueue();
  private final AtomicInteger pendingBytes = new AtomicInteger();
  
  PublishBuffer(int maxPendingBytes) {
    this.maxPendingBytes = maxPendingBytes;
  }
  
  boolean add(byte[] message) {
    int len = message.length;
    
    // Check we can send without violating space restrictions
    if (pendingBytes.getAndAdd(len) > maxPendingBytes) {
      pendingBytes.getAndAdd(-len);
      return false;
    }

    // Enqueue the message
    q.add(message);
    System.out.println("Added pending");
    return true;
  }
  
  void force(byte[] message) {
    pendingBytes.getAndAdd(message.length);
    q.add(message);
    System.out.println("Forced pending");
  }

  byte[] current() {
    return q.peek();
  }

  void completed() {
    byte[] prev = q.poll();
    if (prev != null) {
      pendingBytes.getAndAdd(-prev.length);
    }
  }

  boolean hasMore() {
    return current() != null;
  }

}
