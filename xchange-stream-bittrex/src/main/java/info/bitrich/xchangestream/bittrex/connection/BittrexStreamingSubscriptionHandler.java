package info.bitrich.xchangestream.bittrex.connection;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.LinkedHashMap;
import java.util.Map;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;

import info.bitrich.xchangestream.bittrex.BittrexStreamingService;

public class BittrexStreamingSubscriptionHandler implements SubscriptionHandler1<String> {

  private static final int MESSAGE_SET_CAPACITY = 1_000 * BittrexStreamingService.POOL_SIZE;
  private static final TemporalAmount HISTORY_EXPIRE_TIME = Duration.ofSeconds(2);

  private final SubscriptionHandler1<String> handler;
  private final MessageSet messageDuplicatesSet;

  public BittrexStreamingSubscriptionHandler(SubscriptionHandler1<String> handler) {
    this.handler = handler;
    this.messageDuplicatesSet = new MessageSet();
  }

  @Override
  public void run(String message) {
    if (!alreadyReceived(message)) {
      handler.run(message);
    }
  }

  private boolean alreadyReceived(String message) {
    return messageDuplicatesSet.isDuplicateMessage(message);
  }

  static class MessageSet {
    private final LinkedHashMap<String, LocalDateTime> messagesCollection;

    MessageSet() {
      messagesCollection =
          new LinkedHashMap<String, LocalDateTime>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, LocalDateTime> eldest) {
              return eldest.getValue().isBefore(LocalDateTime.now().minus(HISTORY_EXPIRE_TIME))
                  || size() > MESSAGE_SET_CAPACITY;
            }
          };
    }

    synchronized boolean isDuplicateMessage(String message) {
      return messagesCollection.put(message, LocalDateTime.now()) != null;
    }
  }
}
