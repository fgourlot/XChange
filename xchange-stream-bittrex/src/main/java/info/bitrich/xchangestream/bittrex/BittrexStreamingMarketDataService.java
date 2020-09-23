package info.bitrich.xchangestream.bittrex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;

import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;

/** See https://bittrex.github.io/api/v3#topic-Websocket-Overview */
public class BittrexStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingMarketDataService.class);
  private static final int ORDER_BOOKS_DEPTH = 500;
  private static final Object SUBSCRIBE_LOCK = new Object();
  private static final Object ORDER_BOOKS_LOCK = new Object();

  private final BittrexStreamingService streamingService;
  private final BittrexMarketDataService marketDataService;

  private final ConcurrentMap<CurrencyPair, SequencedOrderBook> sequencedOrderBooks;
  private final ConcurrentMap<CurrencyPair, LinkedList<BittrexOrderBookDeltas>> orderBookDeltasQueue;
  private final ConcurrentMap<CurrencyPair, Subject<OrderBook>> orderBooks;
  private final SubscriptionHandler1<String> orderBooksHandler;
  private final ObjectMapper objectMapper;

  private boolean isOrderbooksChannelSubscribed;

  public BittrexStreamingMarketDataService(
      BittrexStreamingService streamingService, BittrexMarketDataService marketDataService) {
    this.streamingService = streamingService;
    this.marketDataService = marketDataService;
    this.objectMapper = new ObjectMapper();
    this.orderBookDeltasQueue = new ConcurrentHashMap<>();
    this.sequencedOrderBooks = new ConcurrentHashMap<>();
    this.orderBooks = new ConcurrentHashMap<>();
    this.isOrderbooksChannelSubscribed = false;
    this.orderBooksHandler = createHandler();
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
    orderBookDeltasQueue.putIfAbsent(currencyPair, new LinkedList<>());
    if (!isOrderbooksChannelSubscribed) {
      synchronized (SUBSCRIBE_LOCK) {
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
    return null;
  }

  @Override
  public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
    // TODO
    return null;
  }

  /** Subscribes to all of the order books channels available via getting ticker in one go. */
  private void subscribeToOrderBookChannels() {
    List<Ticker> tickers = new ArrayList<>();
    try {
      tickers = this.marketDataService.getTickers(null);
    } catch (IOException e) {
      LOG.error("Could not subscribe to tickers.", e);
    }
    String[] orderBooksChannel =
        tickers.stream()
            .map(ticker -> BittrexUtils.toPairString(ticker.getCurrencyPair()))
            .map(marketName -> "orderbook_" + marketName + "_" + ORDER_BOOKS_DEPTH)
            .toArray(String[]::new);
    streamingService.subscribeToChannelWithHandler(
        orderBooksChannel, "orderbook", this.orderBooksHandler);
    isOrderbooksChannelSubscribed = true;
  }

  /**
   * Creates the handler which will work with the websocket incoming messages.
   *
   * @return the created handler
   */
  private SubscriptionHandler1<String> createHandler() {
    return message -> {
      try {
        BittrexOrderBookDeltas orderBookDeltas =
            objectMapper.readValue(
                EncryptionUtils.decompress(message), BittrexOrderBookDeltas.class);
        CurrencyPair market = BittrexUtils.toCurrencyPair(orderBookDeltas.getMarketSymbol());
        if (orderBooks.containsKey(market)) {
          OrderBook orderBookClone;
          synchronized (ORDER_BOOKS_LOCK) {
            queueOrderBookDeltas(orderBookDeltas, market);
            updateOrderBook(market);
            orderBookClone = cloneOrderBook(market);
          }
          orderBooks.get(market).onNext(orderBookClone);
        }
      } catch (IOException e) {
        LOG.error("Error while decompressing and treating order book update", e);
      }
    };
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
    synchronized (ORDER_BOOKS_LOCK) {
      orderBookClone = cloneOrderBook(market);
    }
    orderBooks.putIfAbsent(market, BehaviorSubject.createDefault(orderBookClone));
  }

  /**
   * Clones an orderbook.
   *
   * @param market the market of the order book
   * @return the cloned order book
   */
  private OrderBook cloneOrderBook(CurrencyPair market) {
    return new OrderBook(
        sequencedOrderBooks.get(market).getOrderBook().getTimeStamp(),
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
    LinkedList<BittrexOrderBookDeltas> deltasQueue = orderBookDeltasQueue.get(market);
    if (deltasQueue.isEmpty()) {
      deltasQueue.add(orderBookDeltas);
    } else {
      int lastSequence = deltasQueue.getLast().getSequence();
      if (lastSequence + 1 == orderBookDeltas.getSequence()) {
        deltasQueue.add(orderBookDeltas);
      } else if (lastSequence + 1 < orderBookDeltas.getSequence()) {
        deltasQueue.clear();
        deltasQueue.add(orderBookDeltas);
      }
    }
  }

  /**
   * Apply the in memory updates to the order book.
   *
   * @param market the order book's market
   * @throws IOException if the order book could not be initialized
   */
  private void updateOrderBook(CurrencyPair market) throws IOException {
    if (needOrderBookInit(market)) {
      initializeOrderBook(market);
    }
    SequencedOrderBook orderBook = sequencedOrderBooks.get(market);
    int lastSequence = Integer.parseInt(orderBook.getSequence());
    LinkedList<BittrexOrderBookDeltas> updatesToApply = orderBookDeltasQueue.get(market);
    updatesToApply.stream()
        .filter(deltas -> deltas.getSequence() > lastSequence)
        .forEach(
            deltas -> {
              OrderBook updatedOrderBook =
                  BittrexStreamingUtils.updateOrderBook(orderBook.getOrderBook(), deltas);
              String sequence = String.valueOf(deltas.getSequence());
              sequencedOrderBooks.put(market, new SequencedOrderBook(sequence, updatedOrderBook));
            });
    updatesToApply.clear();
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
    return orderBookDeltasQueue.get(market).getFirst().getSequence() > currentBookSequence + 1;
  }
}
