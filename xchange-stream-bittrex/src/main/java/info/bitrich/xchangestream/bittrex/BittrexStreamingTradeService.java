package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrder;
import info.bitrich.xchangestream.core.StreamingTradeService;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.knowm.xchange.bittrex.service.BittrexTradeService;
import org.knowm.xchange.bittrex.service.BittrexTradeServiceRaw;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.UserTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;

public class BittrexStreamingTradeService implements StreamingTradeService {
  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingTradeService.class);

  private final BittrexStreamingService bittrexStreamingService;

  private final BittrexTradeService bittrexTradeService;

  /** BittrexBalance queue before sequence number synchronisation */
  private LinkedList<BittrexOrder> bittrexOrdersQueue;

  /** First Sequence Number verification flag */
  private boolean firstSequenceNumberVerified = false;

  /** Current sequence number (to be increased after each message) */
  private int currentSequenceNumber = 0;

  public BittrexStreamingTradeService(
      BittrexStreamingService service, BittrexTradeService bittrexTradeService) {
    this.bittrexStreamingService = service;
    this.bittrexTradeService = bittrexTradeService;
    this.bittrexOrdersQueue = new LinkedList<>();
  }

  @Override
  public Observable<Order> getOrderChanges(CurrencyPair currencyPair, Object... args) {
    return null;
  }

  @Override
  public Observable<UserTrade> getUserTrades(CurrencyPair currencyPair, Object... args) {

    // create result Observable
    Observable<UserTrade> obs;
    obs =
        new Observable<UserTrade>() {
          @Override
          protected void subscribeActual(Observer<? super UserTrade> observer) {
            // create handler for `balance` messages
            SubscriptionHandler1<String> orderHandler =
                message -> {
                  LOG.debug("Incoming order message : {}", message);
                  try {
                    // parse message to BittrexBalance object
                    BittrexOrder bittrexOrder =
                        BittrexStreamingUtils.bittrexOrderMessageToBittrexOrder(message);

                    // check sequence number
                    if (!firstSequenceNumberVerified) {
                      // add to queue for further verifications
                      bittrexOrdersQueue.add(bittrexOrder);
                      // get sequence number reference
                      int ordersSequenceNumber = getSequenceNumberFromRestAPI();
                      verifySequenceNumber(ordersSequenceNumber);
                      observer.onNext(null);
                    } else if (bittrexOrder.getSequence() == (currentSequenceNumber + 1)) {
                      UserTrade userTrade =
                          BittrexStreamingUtils.bittrexOrderToUserTrade(bittrexOrder);
                      LOG.debug(
                          "Emitting Order with ID {} for operation {} {} on price {} for amount {}",
                          userTrade.getOrderId(),
                          userTrade.getType(),
                          userTrade.getCurrencyPair(),
                          userTrade.getPrice(),
                          userTrade.getOriginalAmount());
                      currentSequenceNumber = bittrexOrder.getSequence();
                      observer.onNext(userTrade);
                    } else {
                      LOG.info(
                          "Orders desynchronized ! (sequence number is greater than 1 from the last message), will perform synchronization again");
                      firstSequenceNumberVerified = false;
                      bittrexOrdersQueue.clear();
                      bittrexOrdersQueue.add(bittrexOrder);
                      observer.onNext(null);
                    }
                  } catch (JsonProcessingException e) {
                    e.printStackTrace();
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                };
            bittrexStreamingService.setHandler("order", orderHandler);
          }
        };

    String balanceChannel = "order";
    String[] channels = {balanceChannel};
    LOG.info("Subscribing to channel : {}", balanceChannel);
    this.bittrexStreamingService.subscribeToChannels(channels);
    return obs;
  }

  /**
   * Returns current sequence number reference from Bittrex V3 REST API
   *
   * @return
   * @throws IOException
   */
  private int getSequenceNumberFromRestAPI() throws IOException {
    // get Bittrex Orders from V3 REST API
    BittrexTradeServiceRaw.SequencedOpenOrders sequencedOpenOrders =
        bittrexTradeService.getBittrexSequencedOpenOrders(null);
    // get sequence number reference
    int sequenceNumberReference = Integer.parseInt(sequencedOpenOrders.getSequence());
    LOG.debug("Sequence number reference from V3 REST API (Orders) : {}", sequenceNumberReference);
    return sequenceNumberReference;
  }

  /**
   * Checks sequence number reference (from V3 REST API) against Bittrex order message in queue
   *
   * @param sequenceNumberReference
   */
  private void verifySequenceNumber(int sequenceNumberReference) {
    if (sequenceNumberReference > bittrexOrdersQueue.getFirst().getSequence()) {
      firstSequenceNumberVerified = true;
      currentSequenceNumber = sequenceNumberReference;
      LOG.info("Orders synchronized ! Start sequence number is : {}", currentSequenceNumber);
    }
  }
}
