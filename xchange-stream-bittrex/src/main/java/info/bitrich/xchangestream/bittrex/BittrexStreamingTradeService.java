package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrder;
import info.bitrich.xchangestream.core.StreamingTradeService;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexTradeService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.UserTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingTradeService implements StreamingTradeService {
  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingTradeService.class);
  private static final int MAX_DELTAS_IN_MEMORY = 100_000;

  private final BittrexStreamingService bittrexStreamingService;
  private final BittrexTradeService bittrexTradeService;

  private final Object ordersLock = new Object();
  private final Object subscribeLock = new Object();
  private boolean isOrdersChannelSubscribed;

  private final SubscriptionHandler1<String> orderChangesMessageHandler;
  private final ObjectMapper objectMapper;
  private final Map<CurrencyPair, Subject<Order>> orders;

  public BittrexStreamingTradeService(
      BittrexStreamingService bittrexStreamingService, BittrexTradeService bittrexTradeService) {
    this.bittrexStreamingService = bittrexStreamingService;
    this.bittrexTradeService = bittrexTradeService;
    this.objectMapper = new ObjectMapper();
    this.orderChangesMessageHandler = createOrderChangesMessageHandler();
    this.orders = new ConcurrentHashMap<>();
  }

  @Override
  public Observable<Order> getOrderChanges(CurrencyPair currencyPair, Object... args) {
    orders.putIfAbsent(currencyPair, PublishSubject.create());
    if (!isOrdersChannelSubscribed) {
      synchronized (subscribeLock) {
        if (!isOrdersChannelSubscribed) {
          subscribeToOrdersChannels();
        }
      }
    }
    return orders.get(currencyPair);
  }

  @Override
  public Observable<UserTrade> getUserTrades(CurrencyPair currencyPair, Object... args) {
    // TODO
    return null;
  }

  private SubscriptionHandler1<String> createOrderChangesMessageHandler() {
    return message -> {
      BittrexOrder bittrexOrder =
          BittrexStreamingUtils.bittrexOrderMessageToBittrexOrder(message, objectMapper);
      if (bittrexOrder != null) {
        synchronized (ordersLock) {
          orders
              .get(BittrexUtils.toCurrencyPair(bittrexOrder.getDelta().getMarketSymbol()))
              .onNext(BittrexStreamingUtils.bittrexOrderToOrder(bittrexOrder));
        }
      }
    };
  }

  /** Subscribes to all of the order books channels available via getting ticker in one go. */
  private void subscribeToOrdersChannels() {
    String orderChannel = "order";
    bittrexStreamingService.subscribeToChannelWithHandler(
        new String[] {orderChannel}, "order", this.orderChangesMessageHandler);
    isOrdersChannelSubscribed = true;
  }
}
