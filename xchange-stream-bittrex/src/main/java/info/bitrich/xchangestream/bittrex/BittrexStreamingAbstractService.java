package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscription;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscriptionHandler;
import info.bitrich.xchangestream.bittrex.dto.BittrexSequencedEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedSet;

public abstract class BittrexStreamingAbstractService<T extends BittrexSequencedEntity> {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAbstractService.class);
  protected static final int MAX_DELTAS_IN_MEMORY = 100_000;

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Object subscribeLock = new Object();
  private boolean isChannelSubscribed = false;
  protected BittrexStreamingService bittrexStreamingService;
  protected BittrexStreamingSubscriptionHandler messageHandler;

  protected void subscribeToDataStream(String eventName, String[] channels, boolean authenticated) {
    if (!isChannelSubscribed) {
      synchronized (subscribeLock) {
        if (!isChannelSubscribed) {
          BittrexStreamingSubscription subscription =
              new BittrexStreamingSubscription(eventName, channels, authenticated, messageHandler);
          bittrexStreamingService.subscribeToChannelWithHandler(subscription);
          isChannelSubscribed = true;
        }
      }
    }
  }

  protected BittrexStreamingSubscriptionHandler createMessageHandler(Class<T> bittrexClass) {
    return new BittrexStreamingSubscriptionHandler(
        message ->
            BittrexStreamingUtils.extractBittrexEntity(message, objectMapper.reader(), bittrexClass)
                .filter(this::isAccepted)
                .ifPresent(
                    bittrexEntity -> {
                      if (!isNextSequenceValid(
                          getLastReceivedSequence(bittrexEntity), bittrexEntity.getSequence())) {
                        LOG.info("{} sequence desync!", bittrexClass.getSimpleName());
                        initializeData(bittrexEntity);
                        getDeltaQueue(bittrexEntity).clear();
                      }
                      updateLastReceivedSequence(bittrexEntity);
                      queueDelta(bittrexEntity);
                      updateData(bittrexEntity);
                    }));
  }

  protected abstract boolean isAccepted(T bittrexEntity);

  protected abstract Number getLastReceivedSequence(T bittrexEntity);

  protected abstract SortedSet<T> getDeltaQueue(T bittrexEntity);

  protected abstract void initializeData(T bittrexEntity);

  protected abstract void queueDelta(T bittrexEntity);

  protected abstract void updateData(T bittrexEntity);

  protected abstract void updateLastReceivedSequence(T bittrexEntity);

  protected static boolean isNextSequenceValid(Number previousSequence, Number nextSequence) {
    return previousSequence == null || previousSequence.longValue() + 1 == nextSequence.longValue();
  }

  protected void queueDelta(SortedSet<T> queue, T delta){
    queue.add(delta);
    while (queue.size() > MAX_DELTAS_IN_MEMORY) {
      queue.remove(queue.first());
    }

  }
}
