package info.bitrich.xchangestream.bittrex;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepthV3;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
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

  private final BittrexStreamingService service;
  private final BittrexMarketDataService marketDataService;

  /**
   * OrderBookV3 Cache (requested via Bittrex REST API)
   */
  private HashMap<CurrencyPair, Pair<OrderBook, String>> orderBookCache;

  private LinkedList<BittrexOrderBookDeltas> orderBookDeltasQueue;

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

  /**
   * OrderBook snapshot reference to be updated after each WebSocket message
   */
  OrderBook orderBookReference;

  public BittrexStreamingMarketDataService(
      BittrexStreamingService service, BittrexMarketDataService marketDataService) {
    this.service = service;
    this.marketDataService = marketDataService;
    objectMapper = new ObjectMapper();
    orderBookCache = new HashMap<>();
    orderBookDeltasQueue = new LinkedList<>();
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
                    BittrexOrderBookDeltas orderBookDeltas = objectMapper.readValue(decompressedMessage, BittrexOrderBookDeltas.class);
                    // check sequence before dispatch
                    if (!firstSequenceNumberVerified) {
                      // add to queue for further verifications
                      orderBookDeltasQueue.add(orderBookDeltas);
                      // check first orderbook sequence number
                      orderBookReference = getOrderBookReference(currencyPair);
                    } else if (orderBookDeltas.getSequence() == (currentSequenceNumber + 1)) {
                      LOG.debug(
                          "Emitting OrderBook with sequence {}", orderBookDeltas.getSequence());
                      currentSequenceNumber = orderBookDeltas.getSequence();
                      BittrexStreamingUtils.updateOrderBook(orderBookReference, orderBookDeltas);
                      OrderBook orderBookClone = new OrderBook(null,
                                                               BittrexStreamingUtils.cloneOrders(orderBookReference.getAsks()),
                                                               BittrexStreamingUtils.cloneOrders(orderBookReference.getBids()));
                      Map<String, Object> metadata = Map.of(BittrexDepthV3.SEQUENCE, currentSequenceNumber);
                      orderBookClone.setMetadata(metadata);
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

    String orderBookChannel =
        "orderbook_"
            + currencyPair.base.toString()
            + "-"
            + currencyPair.counter.toString()
            + "_500";
    String[] channels = {orderBookChannel};
    LOG.info("Subscribing to channel : {}", orderBookChannel);
    this.service.subscribeToChannels(channels);

    return obs;
  }


  /**
   * Verify first BittrexOrderBook sequence number with OrderBook V3 sequence number (requested via
   * Bittrex REST API) When sequence number is verified, set current sequence number and return
   * OrderBook snapshot reference
   *
   * @param currencyPair
   * @throws IOException
   */
  private OrderBook getOrderBookReference(CurrencyPair currencyPair) throws IOException {
    if (!orderBookDeltasQueue.isEmpty()) {
      // get OrderBookV3 via REST
      LOG.debug("Getting OrderBook V3 via REST for Currency Pair {} ...", currencyPair);
      Pair<OrderBook, String> orderBookV3 = marketDataService.getOrderBookV3(currencyPair);
      LOG.debug("Received OrderBook V3 for Currency Pair {} : {}", currencyPair, orderBookV3.getLeft());
      LOG.debug("OrderBook V3 Sequence number : {}", orderBookV3.getRight());
      orderBookCache.put(currencyPair, orderBookV3);
      int orderBookV3SequenceNumber = Integer.parseInt(orderBookV3.getRight());
      if (orderBookV3SequenceNumber > orderBookDeltasQueue.getFirst().getSequence()) {
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
