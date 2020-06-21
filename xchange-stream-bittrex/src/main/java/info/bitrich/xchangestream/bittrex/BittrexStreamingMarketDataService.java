package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;

import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBook;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.Observer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepthV3;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BittrexStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingMarketDataService.class);

  private final BittrexStreamingService service;
  private final BittrexMarketDataService bittrexMarketDataService;

  /**
   * OrderBookV3 Cache (requested via Bittrex REST API)
   */
  private HashMap<CurrencyPair, Pair<OrderBook, String>> orderBookV3Cache;

  private LinkedList<BittrexOrderBook> bittrexOrderBookQueue;

  /**
   * Current sequence number (to be increased after each message)
   */
  private int currentSequenceNumber = 0;

  /**
   * First Sequence Number verification flag
   */
  private boolean firstSequenceNumberVerified = false;

  /**
   * Object mapper for JSON parsing
   */
  private ObjectMapper objectMapper;

  OrderBook orderBookReference;

  public BittrexStreamingMarketDataService(BittrexStreamingService service, BittrexMarketDataService bittrexMarketDataService) {
    this.service = service;
    this.bittrexMarketDataService = bittrexMarketDataService;
    objectMapper = new ObjectMapper();
    orderBookV3Cache = new HashMap<>();
    bittrexOrderBookQueue = new LinkedList<>();
    orderBookReference = null;
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {

    // create result Observable
    Observable<OrderBook> obs =
        new Observable<>() {
          @Override
          protected void subscribeActual(Observer<? super OrderBook> observer) {
            // create handler for `orderbook` messages
            SubscriptionHandler1<String> orderBookHandler =
                message -> {
                  LOG.debug("Incoming orderbook message : {}", message);
                  try {
                    String decompressedMessage = EncryptionUtility.decompress(message);
                    LOG.debug("Decompressed orderbook message : {}", decompressedMessage);
                    // parse JSON to Object
                    BittrexOrderBook bittrexOrderBook = objectMapper.readValue(decompressedMessage, BittrexOrderBook.class);
                    // check sequence before dispatch
                    if (!firstSequenceNumberVerified) {
                      // add to queue for further verifications
                      bittrexOrderBookQueue.add(bittrexOrderBook);
                      // check first orderbook sequence number
                      orderBookReference = getOrderBookReference(currencyPair);
                    } else if (bittrexOrderBook.getSequence() == (currentSequenceNumber + 1)) {
                      LOG.debug("Emitting OrderBook with sequence {}", bittrexOrderBook.getSequence());
                      currentSequenceNumber = bittrexOrderBook.getSequence();
                      updateOrderBook(orderBookReference, bittrexOrderBook);
                      OrderBook orderBookClone = cloneOrderBook(orderBookReference);
                      observer.onNext(orderBookClone);
                    }
                  } catch (IOException e) {
                    LOG.error("Error while receiving and treating orderbook message", e);
                    throw new RuntimeException(e);
                  }
                };
            service.setHandler("orderbook", orderBookHandler);
          }
        };

    String orderBookChannel = "orderbook_" + currencyPair.base.toString() + "-" + currencyPair.counter.toString() + "_500";
    String[] channels = {orderBookChannel};
    LOG.info("Subscribing to channel : {}", orderBookChannel);
    this.service.subscribeToChannels(channels);

    return obs;
  }

  private OrderBook cloneOrderBook(OrderBook bookToClone) {
    OrderBook orderBookClone = new OrderBook(bookToClone.getTimeStamp(),
                                             bookToClone.getAsks().stream().map(order -> LimitOrder.Builder.from(order).build()),
                                             bookToClone.getBids().stream().map(order -> LimitOrder.Builder.from(order).build()));
    orderBookClone.setMetadata(Map.of(BittrexDepthV3.SEQUENCE,
                                      bookToClone.getMetadata().get(BittrexDepthV3.SEQUENCE).toString()));
    return orderBookClone;
  }

  /**
   * Update a given OrderBook with Bittrex deltas in a BittrexOrderBook message
   *
   * @param orderBookToUpdate
   * @param updates
   * @return
   */
  protected static OrderBook updateOrderBook(OrderBook orderBookToUpdate, BittrexOrderBook updates) {
    applyUpdates(orderBookToUpdate, updates);
    Map<String, Object> metadata = Map.of(BittrexDepthV3.SEQUENCE, updates.getSequence());
    orderBookToUpdate.setMetadata(metadata);
    return orderBookToUpdate;
  }

  /**
   * Effective orders updates method (add and delete)
   *
   * @param orderBookToUpdate
   * @param updates
   * @param orderType
   * @param market
   */
  private static void applyUpdates(OrderBook orderBookToUpdate, BittrexOrderBookEntry[] updates, Order.OrderType orderType, CurrencyPair market) {
    Arrays.stream(updates).forEach(update -> {
      if (BigDecimal.ZERO.compareTo(update.getQuantity()) == 0) {
        orderBookToUpdate.getOrders(orderType).removeIf(order -> order.getLimitPrice().compareTo(update.getRate()) == 0);
      } else {
        LimitOrder limitOrderUpdate = new LimitOrder.Builder(orderType, market).originalAmount(update.getQuantity()).limitPrice(update.getRate()).build();
        orderBookToUpdate.update(limitOrderUpdate);
      }
    });
  }

  private static void applyUpdates(OrderBook orderBookReference, BittrexOrderBook bittrexOrderBook) {
    CurrencyPair market = BittrexUtils.toCurrencyPair(bittrexOrderBook.getMarketSymbol(), true);
    applyUpdates(orderBookReference, bittrexOrderBook.getAskDeltas(), Order.OrderType.ASK, market);
    applyUpdates(orderBookReference, bittrexOrderBook.getBidDeltas(), Order.OrderType.BID, market);
  }

  /**
   * Verify first BittrexOrderBook sequence number
   * with OrderBook V3 sequence number (requested via Bittrex REST API)
   *
   * @param currencyPair
   * @throws IOException
   */
  private OrderBook getOrderBookReference(CurrencyPair currencyPair) throws IOException {
    if (!bittrexOrderBookQueue.isEmpty()) {
      // get OrderBookV3 via REST
      LOG.debug("Getting OrderBook V3 via REST for Currency Pair {} ...", currencyPair);
      Pair<OrderBook, String> orderBookV3 = bittrexMarketDataService.getOrderBookV3(currencyPair);
      LOG.debug("Received OrderBook V3 for Currency Pair {} : {}", currencyPair, orderBookV3.getLeft());
      LOG.debug("OrderBook V3 Sequence number : {}", orderBookV3.getRight());
      orderBookV3Cache.put(currencyPair, orderBookV3);
      int orderBookV3SequenceNumber = Integer.parseInt(orderBookV3.getRight());
      if (orderBookV3SequenceNumber > bittrexOrderBookQueue.getFirst().getSequence()) {
        LOG.info("Reference verified ! Start sequence number is : {}", orderBookV3SequenceNumber);
        OrderBook bookReference = orderBookV3.getLeft();
        this.firstSequenceNumberVerified = true;
        currentSequenceNumber = orderBookV3SequenceNumber;
        return bookReference;
      }
    }
    return null;
  }

  @Override
  public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
    // TODO
    return null;
  }

  @Override
  public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
    // TODO
    return null;
  }
}
