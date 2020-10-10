package info.bitrich.xchangestream.bittrex.connection;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;

import info.bitrich.xchangestream.bittrex.BittrexStreamingService;

public class BittrexStreamingSubscriptionHandler implements SubscriptionHandler1<String> {

  private static final int MESSAGE_SET_CAPACITY = 1_000 * BittrexStreamingService.POOL_SIZE;
  private static final Duration HISTORICAL_PERIOD = Duration.ofSeconds(10);

  private final SubscriptionHandler1<String> handler;
  private final MessageSet messageDuplicatesSet;

  public BittrexStreamingSubscriptionHandler(SubscriptionHandler1<String> handler) {
    this.handler = handler;
    this.messageDuplicatesSet = new MessageSet();
  }

  @Override
  public synchronized void run(String message) {
    synchronized (this) {
      boolean alreadyReceived = messageDuplicatesSet.isDuplicateMessage(message);
      if (!alreadyReceived) {
        handler.run(message);
      }
    }
  }

  static class MessageSet {
    private final LinkedHashMap<String, Duration> messagesCollection;

    MessageSet() {
      messagesCollection =
          new LinkedHashMap<String, Duration>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Duration> eldest) {
              return now().minus(HISTORICAL_PERIOD).compareTo(eldest.getValue()) > 0
                  || size() > MESSAGE_SET_CAPACITY;
            }
          };
    }

    boolean isDuplicateMessage(String message) {
      return messagesCollection.put(message, now()) != null;
    }

    private static Duration now() {
      return Duration.ofNanos(System.nanoTime());
    }
  }
}
