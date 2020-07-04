package info.bitrich.xchangestream.bittrex;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.core.StreamingAccountService;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingAccountService implements StreamingAccountService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAccountService.class);

  private final BittrexStreamingService service;

  public BittrexStreamingAccountService(BittrexStreamingService service) {
    this.service = service;
  }

  @Override
  public Observable<Balance> getBalanceChanges(Currency currency, Object... args) {

    // create result Observable
    Observable<Balance> obs =
        new Observable<Balance>() {
          @Override
          protected void subscribeActual(Observer<? super Balance> observer) {
            // create handler for `balance` messages
            SubscriptionHandler1<String> balanceHandler =
                message -> {
                  LOG.debug("Incoming balance message : {}", message);
                  Balance balance = BittrexStreamingUtils.bittrexBalanceMessageToBalance(message);
                  LOG.debug(
                      "Emitting Balance on currency {} with {} available on {} total",
                      balance.getCurrency(),
                      balance.getAvailable(),
                      balance.getTotal());
                  observer.onNext(balance);
                };
            service.setHandler("balance", balanceHandler);
          }
        };

    String balanceChannel = "balance";
    String[] channels = {balanceChannel};
    LOG.info("Subscribing to channel : {}", balanceChannel);
    this.service.subscribeToChannels(channels);

    return obs;
  }
}
