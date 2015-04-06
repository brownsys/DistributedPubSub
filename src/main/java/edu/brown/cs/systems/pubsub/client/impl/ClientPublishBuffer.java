package edu.brown.cs.systems.pubsub.client.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Queues;

class ClientPublishBuffer {

  private final int maxPendingBytes;
  private final BlockingQueue<byte[]> q = Queues.newLinkedBlockingQueue();
  private final AtomicInteger pendingBytes = new AtomicInteger();
  
  ClientPublishBuffer(int maxPendingBytes) {
    this.maxPendingBytes = maxPendingBytes;
  }
  
  boolean add(byte[] message) {
    int len = message.length;
    
    // Check we can send without violating space restrictions
    if (pendingBytes.addAndGet(len) > maxPendingBytes) {
      pendingBytes.addAndGet(-len);
      return false;
    }

    // Enqueue the message
    q.add(message);
    return true;
  }
  
  void force(byte[] message) {
    pendingBytes.addAndGet(message.length);
    q.add(message);
    System.out.println("Forced pending");
  }

  byte[] current() {
    return q.peek();
  }

  void completed() {
    byte[] prev = q.poll();
    if (prev != null) {
      pendingBytes.addAndGet(-prev.length);
    }
  }

  boolean hasMore() {
    return current() != null;
  }

}
