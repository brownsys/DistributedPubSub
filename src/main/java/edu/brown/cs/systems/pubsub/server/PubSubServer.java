package edu.brown.cs.systems.pubsub.server;

public interface PubSubServer {

  /**
   * Close the server, stopping any threads associated with the server
   */
  public void shutdown();

  /**
   * Wait for the server thread(s) to terminate. This call will block until
   * interrupted or the server terminates. Clients should call shutdown first to
   * shut down the server, if desired.
   * 
   * @throws InterruptedException
   */
  public void awaitTermination() throws InterruptedException;
  
}
