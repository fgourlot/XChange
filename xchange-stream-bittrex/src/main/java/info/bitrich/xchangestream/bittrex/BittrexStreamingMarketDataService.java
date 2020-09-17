package info.bitrich.xchangestream.bittrex;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

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
import io.reactivex.Observer;

/**
 * See https://bittrex.github.io/api/v3#topic-Websocket-Overview
 */
public class BittrexStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingMarketDataService.class);
  private static final int ORDER_BOOKS_DEPTH = 500;
  private static final Object ORDER_BOOKS_LOCK = new Object();

  private final BittrexStreamingService streamingService;
  private final BittrexMarketDataService marketDataService;

  private final Map<CurrencyPair, SequencedOrderBook> orderBooks;
  private final Map<CurrencyPair, LinkedList<BittrexOrderBookDeltas>> orderBookDeltasQueue;
  private final ObjectMapper objectMapper;

  public BittrexStreamingMarketDataService(
      BittrexStreamingService streamingService, BittrexMarketDataService marketDataService) {
    this.streamingService = streamingService;
    this.marketDataService = marketDataService;
    this.objectMapper = new ObjectMapper();
    this.orderBookDeltasQueue = new HashMap<>();
    this.orderBooks = new HashMap<>();
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
    orderBookDeltasQueue.putIfAbsent(currencyPair, new LinkedList<>());
    return new Observable<OrderBook>() {
      @Override
      protected void subscribeActual(Observer<? super OrderBook> observer) {
        SubscriptionHandler1<String> orderBookHandler =
            message -> {
              try {
                BittrexOrderBookDeltas orderBookDeltas =
                    objectMapper.readValue(
                        EncryptionUtils.decompress(message), BittrexOrderBookDeltas.class);
                CurrencyPair market =
                    BittrexUtils.toCurrencyPair(orderBookDeltas.getMarketSymbol());
                OrderBook orderBookClone;
                synchronized (ORDER_BOOKS_LOCK) {
                  queueOrderBookDeltas(orderBookDeltas, market);
                  updateOrderBook(market);
                  orderBookClone =
                      new OrderBook(
                          orderBooks.get(market).getOrderBook().getTimeStamp(),
                          BittrexStreamingUtils.cloneOrders(
                              orderBooks.get(market).getOrderBook().getAsks()),
                          BittrexStreamingUtils.cloneOrders(
                              orderBooks.get(market).getOrderBook().getBids()));
                }
                observer.onNext(orderBookClone);
              } catch (IOException e) {
                LOG.error("Error while decompressing order book message", e);
              }
            };
        String orderBookChannel =
            "orderbook_"
                + currencyPair.base.getCurrencyCode()
                + "-"
                + currencyPair.counter.getCurrencyCode()
                + "_"
                + ORDER_BOOKS_DEPTH;
        LOG.info("Subscribing to channel : {}", orderBookChannel);
        streamingService.subscribeToChannelWithHandler(orderBookChannel, "orderbook", orderBookHandler);
      }
    };
  }

  /**
   * Queues the order book updates to apply.
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
   * @param market the market
   * @throws IOException if the order book could not be initialized
   */
  private void updateOrderBook(CurrencyPair market) throws IOException {
    if (needOrderBookInit(market)) {
      initializeOrderbook(market);
    }
    SequencedOrderBook orderBook = orderBooks.get(market);
    int lastSequence = Integer.parseInt(orderBook.getSequence());
    LinkedList<BittrexOrderBookDeltas> updatesToApply = orderBookDeltasQueue.get(market);
    updatesToApply.stream()
        .filter(deltas -> deltas.getSequence() > lastSequence)
        .forEach(
            deltas -> {
              OrderBook updatedOrderBook =
                  BittrexStreamingUtils.updateOrderBook(orderBook.getOrderBook(), deltas);
              String sequence = String.valueOf(deltas.getSequence());
              orderBooks.put(market, new SequencedOrderBook(sequence, updatedOrderBook));
            });
    updatesToApply.clear();
  }

  /**
   * Fetches the frst snapshot of an order book.
   * @param market the market
   * @throws IOException if the order book could not be initialized
   */
  private void initializeOrderbook(CurrencyPair market) throws IOException {
    BittrexMarketDataServiceRaw.SequencedOrderBook orderBook =
        marketDataService.getBittrexSequencedOrderBook(
            BittrexUtils.toPairString(market), ORDER_BOOKS_DEPTH);
    orderBooks.put(market, orderBook);
  }

  private boolean needOrderBookInit(CurrencyPair market) {
    SequencedOrderBook orderBook = orderBooks.get(market);
    if (orderBook == null) {
      return true;
    }
    if (orderBookDeltasQueue.get(market).isEmpty()) {
      return false;
    }
    int currentBookSequence = Integer.parseInt(orderBook.getSequence());
    return orderBookDeltasQueue.get(market).getFirst().getSequence() > currentBookSequence + 1;
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
