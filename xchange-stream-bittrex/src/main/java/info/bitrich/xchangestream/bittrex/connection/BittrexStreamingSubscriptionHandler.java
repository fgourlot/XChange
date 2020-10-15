package info.bitrich.xchangestream.bittrex.connection;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.BittrexStreamingService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BittrexStreamingSubscriptionHandler implements SubscriptionHandler1<String> {

  private static final int MESSAGE_SET_CAPACITY = 1_000 * BittrexStreamingService.POOL_SIZE;
  private static final long HISTORICAL_PERIOD = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS);

  private final SubscriptionHandler1<String> handler;
  private final MessageSet messageDuplicatesSet;
  private final Lock lock;

  public BittrexStreamingSubscriptionHandler(SubscriptionHandler1<String> handler) {
    this.handler = handler;
    this.messageDuplicatesSet = new MessageSet();
    this.lock = new ReentrantLock(true);
  }

  @Override
  public void run(String message) {
    lock.lock();
    try {
      if (!isDuplicate(message)) {
        handler.run(message);
      }
    } finally {
      lock.unlock();
    }
  }

  private boolean isDuplicate(String message) {
    return messageDuplicatesSet.isDuplicateMessage(message);
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
