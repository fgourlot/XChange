package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;

import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscription;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw.SequencedOrderBook;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** See https://bittrex.github.io/api/v3#topic-Websocket-Overview */
public class BittrexStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingMarketDataService.class);
  private static final int ORDER_BOOKS_DEPTH = 500;
  private static final int MAX_DELTAS_IN_MEMORY = 1_000;
  private static final int MESSAGE_SET_CAPACITY = 10_000;

  private final BittrexStreamingService streamingService;
  private final BittrexMarketDataService marketDataService;

  private final ConcurrentMap<CurrencyPair, SequencedOrderBook> sequencedOrderBooks;
  private final ConcurrentMap<CurrencyPair, SortedSet<BittrexOrderBookDeltas>> orderBookDeltasQueue;
  private final ConcurrentMap<CurrencyPair, Subject<OrderBook>> orderBooks;
  private final SubscriptionHandler1<String> orderBookMessageHandler;
  private final ObjectMapper objectMapper;
  private final List<CurrencyPair> allMarkets;
  private final Object subscribeLock;
  private final Object orderBooksLock;
  private final MessageSet messageDuplicatesSet;

  private boolean isOrderbooksChannelSubscribed;

  public BittrexStreamingMarketDataService(
      BittrexStreamingService streamingService, BittrexMarketDataService marketDataService) {
    this.subscribeLock = new Object();
    this.orderBooksLock = new Object();
    this.streamingService = streamingService;
    this.marketDataService = marketDataService;
    this.messageDuplicatesSet = new MessageSet();
    this.objectMapper = new ObjectMapper();
    this.allMarkets = getAllMarkets();
    this.orderBookDeltasQueue = new ConcurrentHashMap<>(this.allMarkets.size());
    this.sequencedOrderBooks = new ConcurrentHashMap<>(this.allMarkets.size());
    this.orderBooks = new ConcurrentHashMap<>(this.allMarkets.size());
    this.isOrderbooksChannelSubscribed = false;
    this.orderBookMessageHandler = createOrderBookMessageHandler();
    new Timer()
        .scheduleAtFixedRate(
            new TimerTask() {
              public void run() {
                synchronized (LOCKDEBUG) {
                  Integer duplicateCount =
                      repeatMessageCount.values().stream()
                          .map(AtomicInteger::get)
                          .filter(value -> value > 1)
                          .reduce(0, Integer::sum);
                  LOG.info("duplicate message count: {}", duplicateCount);

                  Integer duplicateCountFiltered =
                      repeatMessageCountFiltered.values().stream()
                          .map(AtomicInteger::get)
                          .filter(value -> value > 1)
                          .reduce(0, Integer::sum);
                  LOG.info("duplicate message count: {}", duplicateCountFiltered);
                }
              }
            },
            0,
            3_000);
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
    orderBookDeltasQueue.putIfAbsent(currencyPair, new TreeSet<>());
    if (!isOrderbooksChannelSubscribed) {
      synchronized (subscribeLock) {
        if (!isOrderbooksChannelSubscribed) {
          subscribeToOrderBookChannels();
        }
      }
    }
    try {
      initializeOrderBook(currencyPair);
    } catch (IOException e) {
      LOG.error("Error while intializing order book", e);
    }
    return orderBooks.get(currencyPair);
  }

  @Override
  public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
    // TODO
    return Observable.empty();
  }

  @Override
  public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
    // TODO
    return Observable.empty();
  }

  /** Subscribes to all of the order books channels available via getting ticker in one go. */
  private void subscribeToOrderBookChannels() {
    String[] orderBooksChannel =
        allMarkets.stream()
            .map(BittrexUtils::toPairString)
            .map(marketName -> "orderbook_" + marketName + "_" + ORDER_BOOKS_DEPTH)
            .toArray(String[]::new);

    BittrexStreamingSubscription subscription =
        new BittrexStreamingSubscription(
            "orderbook", orderBooksChannel, this.orderBookMessageHandler);
    streamingService.subscribeToChannelWithHandler(subscription, true);
    isOrderbooksChannelSubscribed = true;
  }

  // TODO remove
  private Map<String, AtomicInteger> repeatMessageCount = new HashMap<>();
  private Map<String, AtomicInteger> repeatMessageCountFiltered = new HashMap<>();
  private final Object LOCKDEBUG = new Object();
  /**
   * Creates the handler which will work with the websocket incoming messages.
   *
   * @return the created handler
   */
  private SubscriptionHandler1<String> createOrderBookMessageHandler() {
    return message -> {
      // TODO remove
      synchronized (LOCKDEBUG) {
        repeatMessageCount.putIfAbsent(message, new AtomicInteger(0));
        repeatMessageCount.get(message).incrementAndGet();
      }
      if (!alreadyReceived(message)) {
        // TODO remove
        synchronized (LOCKDEBUG) {
          repeatMessageCountFiltered.putIfAbsent(message, new AtomicInteger(0));
          repeatMessageCountFiltered.get(message).incrementAndGet();
        }
        try {
          BittrexOrderBookDeltas orderBookDeltas =
              objectMapper
                  .reader()
                  .readValue(
                      BittrexEncryptionUtils.decompress(message), BittrexOrderBookDeltas.class);
          CurrencyPair market = BittrexUtils.toCurrencyPair(orderBookDeltas.getMarketSymbol());
          if (orderBooks.containsKey(market)) {
            OrderBook orderBookClone = null;
            synchronized (orderBooksLock) {
              queueOrderBookDeltas(orderBookDeltas, market);
              boolean updated = updateOrderBook(market);
              if (updated) {
                orderBookClone = cloneOrderBook(market);
              }
            }
            if (orderBookClone != null) {
              orderBooks.get(market).onNext(orderBookClone);
            }
          }
        } catch (Exception e) {
          LOG.error("Error while decompressing and treating order book update", e);
        }
      }
    };
  }

  private boolean alreadyReceived(String message) {
    return messageDuplicatesSet.isDuplicateMessage(message);
  }

  /**
   * Fetches the first snapshot of an order book.
   *
   * @param market the market
   * @throws IOException if the order book could not be initialized
   */
  private void initializeOrderBook(CurrencyPair market) throws IOException {
    BittrexMarketDataServiceRaw.SequencedOrderBook orderBook =
        marketDataService.getBittrexSequencedOrderBook(
            BittrexUtils.toPairString(market), ORDER_BOOKS_DEPTH);
    sequencedOrderBooks.put(market, orderBook);
    OrderBook orderBookClone;
    synchronized (orderBooksLock) {
      orderBookClone = cloneOrderBook(market);
    }
    orderBooks.putIfAbsent(market, BehaviorSubject.createDefault(orderBookClone).toSerialized());
  }

  /**
   * Clones an orderbook.
   *
   * @param market the market of the order book
   * @return the cloned order book
   */
  private OrderBook cloneOrderBook(CurrencyPair market) {
    return new OrderBook(
        Optional.ofNullable(sequencedOrderBooks.get(market).getOrderBook().getTimeStamp())
            .map(Date::getTime)
            .map(Date::new)
            .orElse(null),
        BittrexStreamingUtils.cloneOrders(sequencedOrderBooks.get(market).getOrderBook().getAsks()),
        BittrexStreamingUtils.cloneOrders(
            sequencedOrderBooks.get(market).getOrderBook().getBids()));
  }

  /**
   * Queues the order book updates to apply.
   *
   * @param orderBookDeltas the order book updates
   * @param market the market
   */
  private void queueOrderBookDeltas(BittrexOrderBookDeltas orderBookDeltas, CurrencyPair market) {
    SortedSet<BittrexOrderBookDeltas> deltasQueue = orderBookDeltasQueue.get(market);
    boolean added = false;
    if (deltasQueue.isEmpty()) {
      added = deltasQueue.add(orderBookDeltas);
    } else {
      int lastSequence = deltasQueue.last().getSequence();
      if (lastSequence + 1 == orderBookDeltas.getSequence()) {
        added = deltasQueue.add(orderBookDeltas);
      } else if (lastSequence + 1 < orderBookDeltas.getSequence()) {
        deltasQueue.clear();
        added = deltasQueue.add(orderBookDeltas);
      }
    }
    if (added && deltasQueue.size() > MAX_DELTAS_IN_MEMORY) {
      deltasQueue.remove(deltasQueue.first());
    }
  }

  /**
   * Apply the in memory updates to the order book.
   *
   * @param market the order book's market
   * @throws IOException if the order book could not be initialized
   */
  private boolean updateOrderBook(CurrencyPair market) throws IOException {
    AtomicBoolean updated = new AtomicBoolean(false);
    if (needOrderBookInit(market)) {
      initializeOrderBook(market);
      updated.set(true);
    }
    SequencedOrderBook orderBook = sequencedOrderBooks.get(market);
    int lastSequence = Integer.parseInt(orderBook.getSequence());
    SortedSet<BittrexOrderBookDeltas> updatesToApply = orderBookDeltasQueue.get(market);
    updatesToApply.stream()
        .filter(deltas -> deltas.getSequence() > lastSequence)
        .forEach(
            deltas -> {
              OrderBook updatedOrderBook =
                  BittrexStreamingUtils.updateOrderBook(orderBook.getOrderBook(), deltas);
              String sequence = String.valueOf(deltas.getSequence());
              sequencedOrderBooks.put(market, new SequencedOrderBook(sequence, updatedOrderBook));
              updated.set(true);
            });
    updatesToApply.clear();
    return updated.get();
  }

  /**
   * Checks if an initial value of an order book is in memory and ready to accept websocket updates
   *
   * @param market the order book's market
   * @return true if the in memory value of the order book is valid
   */
  private boolean needOrderBookInit(CurrencyPair market) {
    SequencedOrderBook orderBook = sequencedOrderBooks.get(market);
    if (orderBook == null) {
      return true;
    }
    if (orderBookDeltasQueue.get(market).isEmpty()) {
      return false;
    }
    int currentBookSequence = Integer.parseInt(orderBook.getSequence());
    return orderBookDeltasQueue.get(market).first().getSequence() > currentBookSequence + 1;
  }

  /**
   * Returns all the bittrex markets.
   *
   * @return the markets
   */
  private List<CurrencyPair> getAllMarkets() {
    try {
      return this.marketDataService.getTickers(null).stream()
          .map(Ticker::getCurrencyPair)
          .collect(Collectors.toList());
    } catch (IOException e) {
      LOG.error("Could not get the tickers.", e);
      return new ArrayList<>();
    }
  }

  static class MessageSet {

    private final LinkedHashMap<String, Boolean> messagesCollection;

    MessageSet() {
      messagesCollection =
          new LinkedHashMap<String, Boolean>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
              return size() > MESSAGE_SET_CAPACITY;
            }
          };
    }

    synchronized boolean isDuplicateMessage(String message) {
      return Boolean.TRUE.equals(messagesCollection.put(message, Boolean.TRUE));
    }
  }
}
