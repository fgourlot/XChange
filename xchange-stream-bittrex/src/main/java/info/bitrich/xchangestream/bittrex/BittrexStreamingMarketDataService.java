package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw.SequencedOrderBook;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static info.bitrich.xchangestream.bittrex.BittrexStreamingUtils.cloneOrderBook;
import static info.bitrich.xchangestream.bittrex.BittrexStreamingUtils.updateOrderBook;

/** See https://bittrex.github.io/api/v3#topic-Websocket-Overview */
public class BittrexStreamingMarketDataService
    extends BittrexStreamingAbstractService<BittrexOrderBookDeltas>
    implements StreamingMarketDataService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingMarketDataService.class);
  private static final int ORDER_BOOKS_DEPTH = 500;

  private final BittrexMarketDataService marketDataService;
  private final Map<CurrencyPair, SequencedOrderBook> sequencedOrderBooks;
  private final Map<CurrencyPair, SortedSet<BittrexOrderBookDeltas>> orderBookDeltasQueue;
  private final ConcurrentMap<CurrencyPair, Subject<OrderBook>> orderBooks;
  private final Map<CurrencyPair, AtomicInteger> lastReceivedDeltaSequences;
  private final String[] orderBooksChannels;
  private final Object orderBooksLock;
  private final Object initLock;

  public BittrexStreamingMarketDataService(
      BittrexStreamingService streamingService, BittrexMarketDataService marketDataService) {
    this.orderBooksLock = new Object();
    this.initLock = new Object();
    this.bittrexStreamingService = streamingService;
    this.marketDataService = marketDataService;
    Set<CurrencyPair> allMarkets = new HashSet<>(getAllMarkets());
    this.orderBookDeltasQueue = new HashMap<>(allMarkets.size());
    this.sequencedOrderBooks = new HashMap<>(allMarkets.size());
    this.orderBooks = new ConcurrentHashMap<>(allMarkets.size());
    this.lastReceivedDeltaSequences = new HashMap<>(allMarkets.size());
    this.orderBooksChannels =
        allMarkets.stream()
            .map(BittrexUtils::toPairString)
            .map(marketName -> "orderbook_" + marketName + "_" + ORDER_BOOKS_DEPTH)
            .toArray(String[]::new);
    this.messageHandler = createMessageHandler(BittrexOrderBookDeltas.class);
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
    synchronized (initLock) {
      orderBookDeltasQueue.putIfAbsent(currencyPair, new TreeSet<>());
      lastReceivedDeltaSequences.putIfAbsent(currencyPair, null);
    }
    subscribeToDataStream("orderbook", orderBooksChannels, false);
    initializeData(new BittrexOrderBookDeltas(BittrexUtils.toPairString(currencyPair)));
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

  @Override
  protected boolean isAccepted(BittrexOrderBookDeltas bittrexEntity) {
    CurrencyPair market = getMarket(bittrexEntity);
    return orderBooks.containsKey(market);
  }

  @Override
  protected Number getLastReceivedSequence(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    return lastReceivedDeltaSequences.get(market);
  }

  @Override
  protected SortedSet<BittrexOrderBookDeltas> getDeltaQueue(
      BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    return orderBookDeltasQueue.get(market);
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
  protected void queueDelta(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    queueDelta(orderBookDeltasQueue.get(market), bittrexOrderBookDeltas);
  }

  private CurrencyPair getMarket(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    return BittrexUtils.toCurrencyPair(bittrexOrderBookDeltas.getMarketSymbol());
  }

  @Override
  protected void updateData(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    SequencedOrderBook orderBook = sequencedOrderBooks.get(market);
    SortedSet<BittrexOrderBookDeltas> updatesToApply = orderBookDeltasQueue.get(market);
    int lastSequence = Integer.parseInt(orderBook.getSequence());

    if (updatesToApply.first().getSequence() - lastSequence > 1) {
      LOG.info("Order book {} desync!", market);
      initializeData(bittrexOrderBookDeltas);
    } else {
      updatesToApply.removeIf(delta -> delta.getSequence() <= lastSequence);
      updatesToApply.forEach(
          deltas -> {
            OrderBook updatedOrderBook = updateOrderBook(orderBook.getOrderBook(), deltas);
            String sequence = String.valueOf(deltas.getSequence());
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

  @Override
  protected void updateLastReceivedSequence(BittrexOrderBookDeltas bittrexOrderBookDeltas) {
    CurrencyPair market = getMarket(bittrexOrderBookDeltas);
    lastReceivedDeltaSequences.put(market, new AtomicInteger(bittrexOrderBookDeltas.getSequence()));
  }
}
