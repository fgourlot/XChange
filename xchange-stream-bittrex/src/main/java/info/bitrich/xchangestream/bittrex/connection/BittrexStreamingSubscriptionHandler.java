package info.bitrich.xchangestream.bittrex.connection;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.BittrexStreamingService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BittrexStreamingSubscriptionHandler implements SubscriptionHandler1<String> {

  private static final int MESSAGE_SET_CAPACITY = 1_000 * BittrexStreamingService.POOL_SIZE;
  private static final long HISTORICAL_PERIOD = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS);

  private final SubscriptionHandler1<String> handler;
  private final MessageSet messageDuplicatesSet;

  public BittrexStreamingSubscriptionHandler(SubscriptionHandler1<String> handler) {
    this.handler = handler;
    this.messageDuplicatesSet = new MessageSet();
  }

  @Override
  public void run(String message) {
    synchronized (this) {
      boolean alreadyReceived = messageDuplicatesSet.isDuplicateMessage(message);
      if (!alreadyReceived) {
        handler.run(message);
      }
    }
  }

  static class MessageSet {
    private final LinkedHashMap<String, Long> messagesCollection;

    MessageSet() {
      messagesCollection =
          new LinkedHashMap<String, Long>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
              return now() - eldest.getValue() > HISTORICAL_PERIOD || size() > MESSAGE_SET_CAPACITY;
            }
          };
    }

    boolean isDuplicateMessage(String message) {
      return messagesCollection.put(message, now()) != null;
    }

    private static long now() {
      return System.nanoTime();
    }
  }
}
