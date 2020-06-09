package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBook;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import io.reactivex.Observer;
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
import java.util.ArrayList;
import java.util.Date;

public class BittrexStreamingMarketDataService implements StreamingMarketDataService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingMarketDataService.class);

  private final BittrexStreamingService service;

  public BittrexStreamingMarketDataService(BittrexStreamingService service) {
    this.service = service;
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
    ObjectMapper objectMapper = new ObjectMapper();
    String orderBookChannel = "orderbook_" + currencyPair.base.toString() + "-" + currencyPair.counter.toString() + "_25";
    String[] channels = {orderBookChannel};
    LOG.info("Subscribing to channel : {}", orderBookChannel);

    Observable<OrderBook> obs =
            new Observable<>() {
              @Override
              protected void subscribeActual(Observer<? super OrderBook> observer) {
                SubscriptionHandler1 orderBookHandler =
                        (SubscriptionHandler1<String>)
                                message -> {
                                  LOG.debug("Incoming orderbook message : {}", message);
                                  try {
                                    String decodedMessage = EncryptionUtility.decompress(message);
                                    LOG.debug("Decompressed orderbook message : {}", decodedMessage);
                                    // parse JSON to Object
                                    BittrexOrderBook bittrexOrderBook =
                                            objectMapper.readValue(decodedMessage, BittrexOrderBook.class);
                                    // forge OrderBook from BittrexOrderBook
                                    OrderBook orderBook = bittrexOrderBookToOrderBook(bittrexOrderBook);
                                    observer.onNext(orderBook);
                                  } catch (IOException e) {
                                    e.printStackTrace();
                                  }
                                };
                service.setHandler("orderbook", orderBookHandler);
              }
            };

    this.service.subscribeToChannels(channels);

    return obs;
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
      LimitOrder askOrder = this.bittrexOrderToLimitOrder(
              Order.OrderType.BID,
              bittrexOrderBook.getMarketSymbol(),
              bittrexOrderBook.getSequence(),
              bidEntry);
      asks.add(askOrder);
    }
    OrderBook orderBook = new OrderBook(new Date(), asks, bids);
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
  private LimitOrder bittrexOrderToLimitOrder(Order.OrderType orderType, CurrencyPair currencyPair, int sequence, BittrexOrderBookEntry bittrexOrderBookEntry) {
    LimitOrder limitOrder =
            new LimitOrder(
                    orderType,
                    BigDecimal.valueOf(bittrexOrderBookEntry.getQuantity()),
                    currencyPair,
                    String.valueOf(sequence),
                    new Date(),
                    BigDecimal.valueOf(bittrexOrderBookEntry.getRate()));
    return limitOrder;
  }
}
