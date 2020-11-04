package info.bitrich.xchangestream.bittrex.services.marketdata;

import static info.bitrich.xchangestream.bittrex.services.utils.BittrexStreamingUtils.cloneOrderBook;
import static info.bitrich.xchangestream.bittrex.services.utils.BittrexStreamingUtils.updateOrderBook;

import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.bittrex.services.BittrexStreamingAbstractService;
import info.bitrich.xchangestream.bittrex.services.BittrexStreamingService;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw.SequencedOrderBook;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingOrderBookService
    extends BittrexStreamingAbstractService<BittrexOrderBookDeltas> {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingOrderBookService.class);

  private static final int ORDER_BOOKS_DEPTH = 500;

  private final BittrexMarketDataService marketDataService;
  private final Map<CurrencyPair, SequencedOrderBook> sequencedOrderBooks;
  private final Map<CurrencyPair, SortedSet<BittrexOrderBookDeltas>> orderBookUpdatesQueue;
  private final ConcurrentMap<CurrencyPair, Subject<OrderBook>> orderBooks;
  private final Map<CurrencyPair, AtomicInteger> lastReceivedUpdateSequences;
  private final String[] orderBooksChannels;
  private final Object orderBooksLock;
  private final Object initLock;

  public BittrexStreamingOrderBookService(
      BittrexStreamingService bittrexStreamingService, BittrexMarketDataService marketDataService) {
    this.orderBooksLock = new Object();
    this.initLock = new Object();
    this.marketDataService = marketDataService;
    this.bittrexStreamingService = bittrexStreamingService;
    Set<CurrencyPair> allMarkets = new HashSet<>(getAllMarkets());
    this.orderBookUpdatesQueue = new HashMap<>(allMarkets.size());
    this.sequencedOrderBooks = new HashMap<>(allMarkets.size());
    this.orderBooks = new ConcurrentHashMap<>(allMarkets.size());
    this.lastReceivedUpdateSequences = new HashMap<>(allMarkets.size());
    this.orderBooksChannels =
        allMarkets.stream()
            .map(BittrexUtils::toPairString)
            .map(marketName -> "orderbook_" + marketName + "_" + ORDER_BOOKS_DEPTH)
            .toArray(String[]::new);
    this.messageHandler = createMessageHandler(BittrexOrderBookDeltas.class);
  }

  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair) {
    synchronized (initLock) {
      orderBookUpdatesQueue.putIfAbsent(currencyPair, new TreeSet<>());
      lastReceivedUpdateSequences.putIfAbsent(currencyPair, null);
    }
    subscribeToDataStream("orderbook", orderBooksChannels, false);
    initializeData(new BittrexOrderBookDeltas(BittrexUtils.toPairString(currencyPair)));
    return orderBooks.get(currencyPair);
  }

  @Override
  protected boolean isAccepted(BittrexOrderBookDeltas bittrexEntity) {
    CurrencyPair market = getMarket(bittrexEntity);
    return orderBooks.containsKey(market);
  }

  @Override
  protected Number getLastReceivedSequence(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    return lastReceivedUpdateSequences.get(market);
  }

  @Override
  protected SortedSet<BittrexOrderBookDeltas> getUpdatesQueue(
      BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    return orderBookUpdatesQueue.get(market);
  }

  @Override
  protected void initializeData(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    LOG.info("Initializing order book {} with a rest call", market);
    try {
      SequencedOrderBook orderBook =
          marketDataService.getBittrexSequencedOrderBook(
              BittrexUtils.toPairString(market), ORDER_BOOKS_DEPTH);
      LOG.debug("Rest sequence for book {} is {}", market, orderBook.getSequence());
      synchronized (orderBooksLock) {
        sequencedOrderBooks.put(market, orderBook);
        OrderBook orderBookClone = cloneOrderBook(sequencedOrderBooks.get(market).getOrderBook());
        if (orderBooks.containsKey(market)) {
          orderBooks.get(market).onNext(orderBookClone);
        } else {
          orderBooks.put(market, BehaviorSubject.createDefault(orderBookClone).toSerialized());
        }
      }
    } catch (IOException e) {
      LOG.error("Error initializing order book {}", market, e);
      initializeData(bittrexOrderBookDeltas);
    }
  }

  @Override
  protected void queueUpdate(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    queueUpdate(orderBookUpdatesQueue.get(market), bittrexOrderBookDeltas);
  }

  @Override
  protected void applyUpdates(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    SequencedOrderBook orderBook = sequencedOrderBooks.get(market);
    SortedSet<BittrexOrderBookDeltas> updatesToApply = orderBookUpdatesQueue.get(market);
    int lastSequence = Integer.parseInt(orderBook.getSequence());

    if (updatesToApply.first().getSequence() - lastSequence > 1) {
      LOG.info("Order book {} desync!", market);
      initializeData(bittrexOrderBookDeltas);
    } else {
      updatesToApply.removeIf(updates -> updates.getSequence() <= lastSequence);
      updatesToApply.forEach(
          updates -> {
            OrderBook updatedOrderBook = updateOrderBook(orderBook.getOrderBook(), updates);
            String sequence = String.valueOf(updates.getSequence());
            sequencedOrderBooks.put(market, new SequencedOrderBook(sequence, updatedOrderBook));
          });
      if (!updatesToApply.isEmpty()) {
        orderBooks
            .get(market)
            .onNext(cloneOrderBook(sequencedOrderBooks.get(market).getOrderBook()));
      }
    }
    updatesToApply.clear();
  }

  private CurrencyPair getMarket(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    return BittrexUtils.toCurrencyPair(bittrexOrderBookDeltas.getMarketSymbol());
  }

  @Override
  protected void updateLastReceivedSequence(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    lastReceivedUpdateSequences.put(
        market, new AtomicInteger(bittrexOrderBookDeltas.getSequence()));
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
