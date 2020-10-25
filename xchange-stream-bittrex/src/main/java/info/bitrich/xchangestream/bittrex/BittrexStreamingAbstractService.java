package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscriptionHandler;
import info.bitrich.xchangestream.bittrex.dto.BittrexSequencedEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedSet;

public abstract class BittrexStreamingAbstractService<T extends BittrexSequencedEntity> {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAbstractService.class);
  private final ObjectMapper objectMapper = new ObjectMapper();

  protected BittrexStreamingSubscriptionHandler createMessageHandler(Class<T> bittrexClass) {
    return new BittrexStreamingSubscriptionHandler(
        message ->
            BittrexStreamingUtils.extractBittrexEntity(message, objectMapper.reader(), bittrexClass)
                .ifPresent(
                    bittrexEntity -> {
                      boolean isSequenceValid =
                          isNextSequenceValid(
                              getLastReceivedSequence(bittrexEntity), bittrexEntity.getSequence());
                      updateLastReceivedSequence(bittrexEntity);
                      if (isAccepted(bittrexEntity)) {
                        if (!isSequenceValid) {
                          LOG.info("{} sequence desync!", bittrexClass.getSimpleName());
                          initializeData(bittrexEntity);
                          getDeltaQueue(bittrexEntity).clear();
                        }
                        queueDelta(bittrexEntity);
                        updateData(bittrexEntity);
                      }
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
}
