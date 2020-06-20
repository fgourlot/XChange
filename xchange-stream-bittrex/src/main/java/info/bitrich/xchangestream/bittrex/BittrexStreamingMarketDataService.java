package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBook;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.apache.commons.lang3.tuple.Pair;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepthV3;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.OrderBookUpdate;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

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
                SubscriptionHandler1 orderBookHandler = (SubscriptionHandler1<String>)
                        message -> {
                          LOG.debug("Incoming orderbook message : {}", message);
                          try {
                            String decompressedMessage = EncryptionUtility.decompress(message);
                            LOG.debug("Decompressed orderbook message : {}", decompressedMessage);
                            // parse JSON to Object
                            BittrexOrderBook bittrexOrderBook =
                                    objectMapper.readValue(decompressedMessage, BittrexOrderBook.class);
                            // check sequence before dispatch
                            if (!firstSequenceNumberVerified) {
                              // add to queue for further verifications
                              bittrexOrderBookQueue.add(bittrexOrderBook);
                              // check first orderbook sequence number
                              orderBookReference = getOrderBookReference(currencyPair);
                            } else if (bittrexOrderBook.getSequence() == (currentSequenceNumber + 1)) {
                              LOG.debug("Emitting OrderBook with sequence {}", bittrexOrderBook.getSequence());
                              currentSequenceNumber = bittrexOrderBook.getSequence();
                              performNextUpdate(observer, bittrexOrderBook);
                            }
                          } catch (IOException e) {
                            e.printStackTrace();
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

  private void performNextUpdate(Observer<? super OrderBook> observer, BittrexOrderBook bittrexOrderBook) {
    if (orderBookReference != null) {

      // update bids
      for (BittrexOrderBookEntry bidEntry : bittrexOrderBook.getBidDeltas()) {
        // remove bids of quantity 0
        if (bidEntry.getQuantity() == 0) {
          int bidIndex = 0;
          for(LimitOrder bidOrder : orderBookReference.getBids()) {
            if (bidOrder.getLimitPrice() == BigDecimal.valueOf(bidEntry.getRate())) {
              orderBookReference.getBids().remove(bidIndex);
            }
            bidIndex++;
          }
        } else {
          OrderBookUpdate bidUpdate = new OrderBookUpdate(
                  Order.OrderType.BID,
                  BigDecimal.valueOf(bidEntry.getQuantity()),
                  new CurrencyPair(bittrexOrderBook.getMarketSymbol().replace("-", "/")),
                  BigDecimal.valueOf(bidEntry.getRate()),
                  new Date(),
                  BigDecimal.valueOf(bidEntry.getQuantity())
          );
          orderBookReference.update(bidUpdate);
        }
      }

      // update asks
      for (BittrexOrderBookEntry askEntry : bittrexOrderBook.getAskDeltas()) {
        // remove asks of quantity 0
        if (askEntry.getQuantity() == 0) {
          int askIndex = 0;
          for(LimitOrder askOrder : orderBookReference.getAsks()) {
            if (askOrder.getLimitPrice() == BigDecimal.valueOf(askEntry.getRate())) {
              orderBookReference.getAsks().remove(askIndex);
            }
            askIndex++;
          }
        } else {
          // TODO
        }
      }

      // set metadata
      HashMap<String, Object> metadata = new HashMap<>();
      metadata.put(BittrexDepthV3.SEQUENCE, bittrexOrderBook.getSequence());
      orderBookReference.setMetadata(metadata);

      observer.onNext(orderBookReference);
    }
  }

  /**
   * Verify first BittrexOrderBook sequence number
   * with OrderBook V3 sequence number (requested via Bittrex REST API)
   * @param currencyPair
   * @throws IOException
   */
  private OrderBook getOrderBookReference(CurrencyPair currencyPair) throws IOException {
    if (bittrexOrderBookQueue.size() > 0) {
      // get OrderBookV3 via REST
      LOG.debug("Getting OrderBook V3 via REST for Currency Pair {} ...", currencyPair);
      Pair<OrderBook, String> orderBookV3 = bittrexMarketDataService.getOrderBookV3(currencyPair);
      LOG.debug("Received OrderBook V3 for Currency Pair {} : {}", currencyPair, orderBookV3.getLeft());
      LOG.debug("OrderBook V3 Sequence number : {}", orderBookV3.getRight());
      orderBookV3Cache.put(currencyPair, orderBookV3);
      int orderBookV3SequenceNumber = Integer.parseInt(orderBookV3.getRight());
      if (orderBookV3SequenceNumber > bittrexOrderBookQueue.getFirst().getSequence()) {
        LOG.info("Reference verified ! Start sequence number is : {}", orderBookV3SequenceNumber);
        OrderBook orderBookReference = orderBookV3.getLeft();
        this.firstSequenceNumberVerified = true;
        currentSequenceNumber = orderBookV3SequenceNumber;
        return orderBookReference;
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

  /**
   * Check if Sequence number is correct
   * it has to be higher than the sequence number reference (from OrderBoook V3 REST API)
   * and +1 higher than current sequence number (from previous message)
   * @param bittrexOrderBook
   * @return
   */
  private boolean isSequenceNumberCorrect(CurrencyPair currencyPair, BittrexOrderBook bittrexOrderBook) {
//    if (orderBookV3Cache.get(currencyPair) != null) {
//      String orderBookV3Right = orderBookV3Cache.get(bittrexOrderBook.getMarketSymbol()).getRight();
//      int orderBookV3Sequence = Integer.parseInt(orderBookV3Right);
//    LOG.info("Fgo {} ", orderBookV3Sequence);

    return bittrexOrderBook.getSequence() == (currentSequenceNumber + 1);
//      return (orderBookV3Sequence < bittrexOrderBook.getSequence()
//              && bittrexOrderBook.getSequence() == (currentSequence + 1));
//    } else {
//      LOG.error("order book v3 null {}", orderBookV3Cache.get(bittrexOrderBook.getMarketSymbol()));
//    }
//    return false;
  }

  /**
   * Map a BittrexOrderBook to OrderBook
   *
   * @param bittrexOrderBook
   * @return
   */
  private OrderBook bittrexOrderBookToOrderBook(BittrexOrderBook bittrexOrderBook) {

    ArrayList<LimitOrder> asks = new ArrayList<LimitOrder>();
    ArrayList<LimitOrder> bids = new ArrayList<LimitOrder>();

    // asks
    for (BittrexOrderBookEntry askEntry : bittrexOrderBook.getAskDeltas()) {
      LimitOrder askOrder = this.bittrexOrderToLimitOrder(
              Order.OrderType.ASK,
              bittrexOrderBook.getMarketSymbol(),
              bittrexOrderBook.getSequence(),
              askEntry);
      asks.add(askOrder);
    }

    // bids
    for (BittrexOrderBookEntry bidEntry : bittrexOrderBook.getAskDeltas()) {
      LimitOrder bidOrder = this.bittrexOrderToLimitOrder(
              Order.OrderType.BID,
              bittrexOrderBook.getMarketSymbol(),
              bittrexOrderBook.getSequence(),
              bidEntry);
      asks.add(bidOrder);
    }

    OrderBook orderBook = new OrderBook(new Date(), asks, bids);

    // set metadata
    HashMap<String, Object> metadata = new HashMap<>();
    metadata.put(BittrexDepthV3.SEQUENCE, bittrexOrderBook.getSequence());
    orderBook.setMetadata(metadata);

    return orderBook;
  }

  /**
   * Map a Bittrex order to LimitOrder
   *
   * @param orderType
   * @param currencyPair
   * @param sequence
   * @param bittrexOrderBookEntry
   * @return
   */
  private LimitOrder bittrexOrderToLimitOrder(Order.OrderType orderType, String currencyPair, int sequence, BittrexOrderBookEntry bittrexOrderBookEntry) {
    LimitOrder limitOrder =
            new LimitOrder(
                    orderType,
                    BigDecimal.valueOf(bittrexOrderBookEntry.getQuantity()),
                    new CurrencyPair(currencyPair.replace("-", "/")),
                    String.valueOf(sequence),
                    new Date(),
                    BigDecimal.valueOf(bittrexOrderBookEntry.getRate()));
    return limitOrder;
  }
}
