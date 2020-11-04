package info.bitrich.xchangestream.bittrex.connection;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.services.BittrexStreamingService;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingSubscriptionHandler
    implements SubscriptionHandler1<String>, AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingSubscriptionHandler.class);

  private static final long HISTORICAL_PERIOD = TimeUnit.NANOSECONDS.convert(5, TimeUnit.SECONDS);
  private static final long MESSAGE_SET_CAPACITY =
      1_000 * HISTORICAL_PERIOD * BittrexStreamingService.POOL_SIZE;

  private final Consumer<String> messageConsumer;
  private final MessageSet messageDuplicatesSet;
  private final BlockingQueue<String> messageQueue;
  private volatile boolean runConsumer;

  public BittrexStreamingSubscriptionHandler(Consumer<String> messageConsumer) {
    this.messageConsumer = messageConsumer;
    this.messageDuplicatesSet = new MessageSet();
    this.messageQueue = new LinkedBlockingQueue<>();
    startMessageConsumer();
  }

  private void startMessageConsumer() {
    this.runConsumer = true;
    new Thread(
            () -> {
              while (runConsumer) getNextMessage().ifPresent(messageConsumer);
            })
        .start();
  }

  private Optional<String> getNextMessage() {
    try {
      String message = messageQueue.take();
      while (isDuplicate(message)) {
        message = messageQueue.take();
      }
      return Optional.of(message);
    } catch (InterruptedException e) {
      LOG.error("Message consumer error", e);
      Thread.currentThread().interrupt();
    }
    return Optional.empty();
  }

  @Override
  public void run(String message) {
    messageQueue.add(message);
  }

  private boolean isDuplicate(String message) {
    return messageDuplicatesSet.isDuplicateMessage(message);
  }

  @Override
  public void close() {
    runConsumer = false;
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
