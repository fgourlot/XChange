package info.bitrich.xchangestream.bittrex;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.core.StreamingTradeService;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.UserTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingTradeService implements StreamingTradeService {
  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingTradeService.class);

  private final BittrexStreamingService service;

  public BittrexStreamingTradeService(BittrexStreamingService service) {
    this.service = service;
  }

  @Override
  public Observable<Order> getOrderChanges(CurrencyPair currencyPair, Object... args) {
    return null;
  }

  @Override
  public Observable<UserTrade> getUserTrades(CurrencyPair currencyPair, Object... args) {

    // create result Observable
    Observable<UserTrade> obs;
    obs =
        new Observable<UserTrade>() {
          @Override
          protected void subscribeActual(Observer<? super UserTrade> observer) {
            // create handler for `balance` messages
            SubscriptionHandler1<String> orderHandler =
                message -> {
                  LOG.debug("Incoming order message : {}", message);
                  UserTrade userTrade =
                      BittrexStreamingUtils.bittrexOrderMessageToUserTrade(message);
                  LOG.debug(
                      "Emitting Order with ID {} for operation {} {} on price {} for amount {}",
                      userTrade.getOrderId(),
                      userTrade.getType(),
                      userTrade.getCurrencyPair(),
                      userTrade.getPrice(),
                      userTrade.getOriginalAmount());
                  observer.onNext(userTrade);
                };
            service.setHandler("order", orderHandler);
          }
        };

    String balanceChannel = "order";
    String[] channels = {balanceChannel};
    LOG.info("Subscribing to channel : {}", balanceChannel);
    this.service.subscribeToChannels(channels);
    return obs;
  }
}
