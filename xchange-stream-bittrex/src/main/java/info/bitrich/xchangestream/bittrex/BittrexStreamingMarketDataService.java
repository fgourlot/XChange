package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscription;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscriptionHandler;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw.SequencedOrderBook;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** See https://bittrex.github.io/api/v3#topic-Websocket-Overview */
public class BittrexStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingMarketDataService.class);
  private static final int ORDER_BOOKS_DEPTH = 500;
  private static final int MAX_DELTAS_IN_MEMORY = 1_000;

  private final BittrexStreamingService streamingService;
  private final BittrexMarketDataService marketDataService;

  private final Map<CurrencyPair, SequencedOrderBook> sequencedOrderBooks;
  private final Map<CurrencyPair, SortedSet<BittrexOrderBookDeltas>> orderBookDeltasQueue;
  private final ConcurrentMap<CurrencyPair, Subject<OrderBook>> orderBooks;
  private final BittrexStreamingSubscriptionHandler orderBookMessageHandler;
  private final ObjectMapper objectMapper;
  private final Set<CurrencyPair> allMarkets;
  private final AtomicBoolean isOrderbooksChannelSubscribed;

  private final Object subscribeLock;
  private final Object orderBooksLock;

  public BittrexStreamingMarketDataService(
      BittrexStreamingService streamingService, BittrexMarketDataService marketDataService) {
    this.subscribeLock = new Object();
    this.orderBooksLock = new Object();
    this.streamingService = streamingService;
    this.marketDataService = marketDataService;
    this.objectMapper = new ObjectMapper();
    this.allMarkets = new HashSet<>(getAllMarkets());
    this.orderBookDeltasQueue = new HashMap<>(this.allMarkets.size());
    this.sequencedOrderBooks = new HashMap<>(this.allMarkets.size());
    this.orderBooks = new ConcurrentHashMap<>(this.allMarkets.size());
    this.isOrderbooksChannelSubscribed = new AtomicBoolean(false);
    this.orderBookMessageHandler = createOrderBookMessageHandler();
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
    orderBookDeltasQueue.putIfAbsent(currencyPair, new TreeSet<>());
    if (!isOrderbooksChannelSubscribed.get()) {
      synchronized (subscribeLock) {
        if (!isOrderbooksChannelSubscribed.get()) {
          subscribeToOrderBookChannels();
        }
      }
    }
    try {
      initializeOrderBook(currencyPair);
    } catch (IOException e) {
      LOG.error("Error while intializing order book", e);
      getOrderBook(currencyPair, args);
    }
    return orderBooks.get(currencyPair);
  }

  @Override
  public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
    throw new NotYetImplementedForExchangeException();
  }

  @Override
  public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
    throw new NotYetImplementedForExchangeException();
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
            "orderbook", orderBooksChannel, false, this.orderBookMessageHandler);
    streamingService.subscribeToChannelWithHandler(subscription);
    isOrderbooksChannelSubscribed.set(true);
  }

  /**
   * Creates the handler which will work with the websocket incoming messages.
   *
   * @return the created handler
   */
  private BittrexStreamingSubscriptionHandler createOrderBookMessageHandler() {
    return new BittrexStreamingSubscriptionHandler(
        message -> {
          try {
            BittrexOrderBookDeltas orderBookDeltas =
                objectMapper
                    .reader()
                    .readValue(
                        BittrexEncryptionUtils.decompress(message), BittrexOrderBookDeltas.class);
            CurrencyPair market = BittrexUtils.toCurrencyPair(orderBookDeltas.getMarketSymbol());
            if (orderBooks.containsKey(market)) {
              queueOrderBookDeltas(orderBookDeltas, market);
              applyUpdates(market);
            }
          } catch (IOException ioe) {
            LOG.error("Error while decompressing order book update", ioe);
          } catch (Exception e) {
            LOG.error("Error while treating order book update", e);
          }
        });
  }

  /**
   * Fetches the first snapshot of an order book.
   *
   * @param market the market
   * @throws IOException if the order book could not be initialized
   */
  private void initializeOrderBook(CurrencyPair market) throws IOException {
    LOG.info("Initializing order book {} with a rest call", market);
    BittrexMarketDataServiceRaw.SequencedOrderBook orderBook =
        marketDataService.getBittrexSequencedOrderBook(
            BittrexUtils.toPairString(market), ORDER_BOOKS_DEPTH);
    LOG.info("Rest sequence for book {} is {}", market, orderBook.getSequence());
    synchronized (orderBooksLock) {
      sequencedOrderBooks.put(market, orderBook);
      OrderBook orderBookClone = cloneOrderBook(market);
      if (orderBooks.containsKey(market)) {
        orderBooks.get(market).onNext(orderBookClone);
      } else {
        orderBooks.put(market, BehaviorSubject.createDefault(orderBookClone).toSerialized());
      }
    }
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
    if (deltasQueue.isEmpty()) {
      deltasQueue.add(orderBookDeltas);
    } else {
      int lastSequence = deltasQueue.last().getSequence();
      if (lastSequence + 1 == orderBookDeltas.getSequence()) {
        deltasQueue.add(orderBookDeltas);
      } else if (lastSequence + 1 < orderBookDeltas.getSequence()) {
        LOG.info(
            "sequence desync: last: {}, current: {}", lastSequence, orderBookDeltas.getSequence());
        deltasQueue.clear();
        deltasQueue.add(orderBookDeltas);
      }
    }
    while (deltasQueue.size() > MAX_DELTAS_IN_MEMORY) {
      deltasQueue.remove(deltasQueue.first());
    }
  }

  /**
   * Apply the in memory updates to the order book.
   *
   * @param market the order book's market
   * @throws IOException if the order book could not be initialized
   */
  private void applyUpdates(CurrencyPair market) throws IOException {
    if (needOrderBookInit(market)) {
      initializeOrderBook(market);
    }
    SequencedOrderBook orderBook = sequencedOrderBooks.get(market);
    int lastSequence = Integer.parseInt(orderBook.getSequence());
    SortedSet<BittrexOrderBookDeltas> updatesToApply = orderBookDeltasQueue.get(market);
    updatesToApply.removeIf(delta -> delta.getSequence() <= lastSequence);
    if (!updatesToApply.isEmpty()) {
      updatesToApply.forEach(
          deltas -> {
            OrderBook updatedOrderBook =
                BittrexStreamingUtils.updateOrderBook(orderBook.getOrderBook(), deltas);
            String sequence = String.valueOf(deltas.getSequence());
            sequencedOrderBooks.put(market, new SequencedOrderBook(sequence, updatedOrderBook));
          });
      orderBooks.get(market).onNext(cloneOrderBook(market));
      updatesToApply.clear();
    }
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
      LOG.info("Need order book {} init because it has never been initialized", market);
      return true;
    }
    if (orderBookDeltasQueue.get(market).isEmpty()) {
      return false;
    }
    int currentBookSequence = Integer.parseInt(orderBook.getSequence());
    int firstDeltaSequence = orderBookDeltasQueue.get(market).first().getSequence();
    boolean sequenceDesync = firstDeltaSequence - currentBookSequence > 1;
    if (sequenceDesync) {
      LOG.info(
          "Resyncing book {} resync (sequence to apply: {}, current: {})",
          market,
          firstDeltaSequence,
          currentBookSequence);
    }
    return sequenceDesync;
  }

  /**
   * Returns all the bittrex markets.
   *
   * @return the markets
   */
  private Set<CurrencyPair> getAllMarkets() {
    try {
      return this.marketDataService.getTickers(null).stream()
          .map(Ticker::getInstrument)
          .filter(CurrencyPair.class::isInstance)
          .map(CurrencyPair.class::cast)
          .collect(Collectors.toSet());
    } catch (IOException e) {
      LOG.error("Could not get the tickers.", e);
      return new HashSet<>();
    }
  }
}
