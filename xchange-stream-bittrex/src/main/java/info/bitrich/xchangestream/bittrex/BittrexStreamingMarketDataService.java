package info.bitrich.xchangestream.bittrex;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

public class BittrexStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingMarketDataService.class);
  public static final int ORDER_BOOKS_DEPTH = 500;

  private final BittrexStreamingService service;
  private final BittrexMarketDataService marketDataService;

  /** OrderBookV3 Cache (requested via Bittrex REST API) */
  private Map<CurrencyPair, SequencedOrderBook> orderBooks;

  /** In memory book updates, to apply when we get the rest value of the books * */
  private Map<CurrencyPair, LinkedList<BittrexOrderBookDeltas>> orderBookDeltasQueue;

  /** Object mapper for JSON parsing */
  private ObjectMapper objectMapper;

  private static final Object ORDER_BOOKS_LOCK = new Object();

  public BittrexStreamingMarketDataService(
      BittrexStreamingService service, BittrexMarketDataService marketDataService) {
    this.service = service;
    this.marketDataService = marketDataService;
    objectMapper = new ObjectMapper();
    orderBookDeltasQueue = new HashMap<>();
    orderBooks = new HashMap<>();
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
    orderBookDeltasQueue.putIfAbsent(currencyPair, new LinkedList<>());
    // create result Observable
    return new Observable<OrderBook>() {
      @Override
      protected void subscribeActual(Observer observer) {
        // create handler for `orderbook` messages
        SubscriptionHandler1<String> orderBookHandler =
            message -> {
              try {
                // Decompress and store message
                BittrexOrderBookDeltas orderBookDeltas =
                    objectMapper.readValue(
                        EncryptionUtils.decompress(message), BittrexOrderBookDeltas.class);
                CurrencyPair market =
                    BittrexUtils.toCurrencyPair(orderBookDeltas.getMarketSymbol());
                synchronized (ORDER_BOOKS_LOCK) {
                  addOrderBookDeltas(orderBookDeltas, market);
                  applyOrderBooksUpdates(orderBookDeltas, market);
                  OrderBook orderBookClone =
                      new OrderBook(
                          null,
                          BittrexStreamingUtils.cloneOrders(
                              orderBooks.get(market).getOrderBook().getAsks()),
                          BittrexStreamingUtils.cloneOrders(
                              orderBooks.get(market).getOrderBook().getBids()));
                  observer.onNext(orderBookClone);
                }
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
        service.subscribeToChannelWithHandler(orderBookChannel, "orderbook", orderBookHandler);
      }
    };
  }

  private void addOrderBookDeltas(BittrexOrderBookDeltas orderBookDeltas, CurrencyPair market) {
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

  private void applyOrderBooksUpdates(BittrexOrderBookDeltas orderBookDeltas, CurrencyPair market)
      throws IOException {
    if (needOrderBookInit(market)) {
      initializeOrderbook(market);
    }
    SequencedOrderBook orderBook = orderBooks.get(market);
    int lastSequence = Integer.parseInt(orderBook.getSequence());
    LinkedList<BittrexOrderBookDeltas> updatesToApply = orderBookDeltasQueue.get(market);

    // Apply updates
    updatesToApply.stream()
        .filter(deltas -> deltas.getSequence() > lastSequence)
        .forEach(
            deltas -> {
              OrderBook updatedOrderBook =
                  BittrexStreamingUtils.updateOrderBook(orderBook.getOrderBook(), orderBookDeltas);
              String sequence = String.valueOf(orderBookDeltas.getSequence());
              orderBooks.put(market, new SequencedOrderBook(sequence, updatedOrderBook));
            });
    updatesToApply.clear();
  }

  private void initializeOrderbook(CurrencyPair market) throws IOException {
    // Get OrderBookV3 via REST
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
