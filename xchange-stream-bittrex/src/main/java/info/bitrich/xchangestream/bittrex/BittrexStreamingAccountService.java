package info.bitrich.xchangestream.bittrex;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.core.StreamingAccountService;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.bittrex.service.BittrexAccountServiceRaw;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BittrexStreamingAccountService implements StreamingAccountService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAccountService.class);

  private final BittrexStreamingService bittrexStreamingService;
  private final BittrexAccountService bittrexAccountService;

  /** Current sequence number (to be increased after each message) */
  private Integer currentSequenceNumber;

  private final Map<Currency, Balance> balances;

  private final Object lock = new Object();

  public BittrexStreamingAccountService(
      BittrexStreamingService bittrexStreamingService,
      BittrexAccountService bittrexAccountService) {
    this.bittrexStreamingService = bittrexStreamingService;
    this.bittrexAccountService = bittrexAccountService;
    this.balances = new HashMap<>();
  }

  @Override
  public Observable<Balance> getBalanceChanges(Currency currency, Object... args) {

    // create result Observable
    Observable<Balance> obs =
        new Observable<Balance>() {
          @Override
          protected void subscribeActual(Observer observer) {
            // create handler for `balance` messages
            SubscriptionHandler1<String> balanceHandler =
                message -> {
                  synchronized (lock) {
                    LOG.debug("Incoming balance message : {}", message);
                    // parse message to BittrexBalance object
                    BittrexBalance bittrexBalance =
                        BittrexStreamingUtils.bittrexBalanceMessageToBittrexBalance(message);
                    if (isStreamBalanceNotSynchronized(bittrexBalance)) {
                      restFillBalances();
                    } else if (bittrexBalance.getSequence() > currentSequenceNumber) {
                      LOG.debug("Applying balance update");
                      Balance balance =
                          BittrexStreamingUtils.bittrexBalanceToBalance(bittrexBalance);
                      Currency currency = bittrexBalance.getDelta().getCurrencySymbol();
                      balances.put(currency, balance);
                      currentSequenceNumber = bittrexBalance.getSequence();
                    } else {
                      LOG.debug(
                          "Ignoring obsolete balance update of sequence {}, current sequence is {}",
                          bittrexBalance.getSequence(),
                          currentSequenceNumber);
                    }
                    Optional.ofNullable(balances.get(currency))
                        .ifPresent(
                            balance -> observer.onNext(Balance.Builder.from(balance).build()));
                  }
                };
            bittrexStreamingService.setHandler("balance", balanceHandler);
          }
        };
    String balanceChannel = "balance";
    String[] channels = {balanceChannel};
    LOG.info("Subscribing to channel : {}", balanceChannel);
    this.bittrexStreamingService.subscribeToChannels(channels);
    return obs;
  }

  private boolean isStreamBalanceNotSynchronized(BittrexBalance bittrexBalance) {
    return currentSequenceNumber == null
        || bittrexBalance.getSequence() - currentSequenceNumber > 1;
  }

  private void restFillBalances() {
    LOG.debug("Synchronizing balances with rest call");
    balances.clear();
    try {
      BittrexAccountServiceRaw.SequencedBalances sequencedBalances =
          bittrexAccountService.getBittrexSequencedBalances();
      balances.putAll(sequencedBalances.getBalances());
      currentSequenceNumber = Integer.parseInt(sequencedBalances.getSequence());
      LOG.debug("Fetched balances with rest call, sequence = {}", currentSequenceNumber);
    } catch (IOException e) {
      LOG.error("Error rest fetching balances", e);
    }
  }
}
