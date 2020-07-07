package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.LinkedList;

public class BittrexStreamingAccountService implements StreamingAccountService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAccountService.class);

  private final BittrexStreamingService bittrexStreamingService;
  private final BittrexAccountService bittrexAccountService;

  /** BittrexBalance queue before sequence number synchronisation */
  private LinkedList<BittrexBalance> bittrexBalancesQueue;

  /** First Sequence Number verification flag */
  private boolean firstSequenceNumberVerified = false;

  /** Current sequence number (to be increased after each message) */
  private int currentSequenceNumber = 0;

  public BittrexStreamingAccountService(
      BittrexStreamingService bittrexStreamingService,
      BittrexAccountService bittrexAccountService) {
    this.bittrexStreamingService = bittrexStreamingService;
    this.bittrexAccountService = bittrexAccountService;
    this.bittrexBalancesQueue = new LinkedList<>();
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
                  try {
                    // parse message to BittrexBalance object
                    BittrexBalance bittrexBalance =
                        BittrexStreamingUtils.bittrexBalanceMessageToBittrexBalance(message);

                    // check sequence number
                    if (!firstSequenceNumberVerified) {
                      // add to queue for further verifications
                      bittrexBalancesQueue.add(bittrexBalance);
                      // get Bittrex Balances from V3 REST API
                      BittrexAccountServiceRaw.SequencedBalances sequencedBalances =
                          bittrexAccountService.getBittrexSequencedBalances();
                      // get sequence number reference
                      int balancesSequenceNumber =
                          Integer.parseInt(sequencedBalances.getSequence());

                      // check sequence number reference vs first balance message sequence number
                      if (balancesSequenceNumber > bittrexBalancesQueue.getFirst().getSequence()) {
                        firstSequenceNumberVerified = true;
                        currentSequenceNumber = balancesSequenceNumber;
                        LOG.info(
                            "Balances synchronized ! Start sequence number is : {}",
                            currentSequenceNumber);
                      }
                    } else if (bittrexBalance.getSequence() == (currentSequenceNumber + 1)) {
                      Balance balance =
                          BittrexStreamingUtils.bittrexBalanceToBalance(bittrexBalance);
                      LOG.debug(
                          "Emitting Balance on currency {} with {} available on {} total",
                          balance.getCurrency(),
                          balance.getAvailable(),
                          balance.getTotal());
                      currentSequenceNumber = bittrexBalance.getSequence();
                      observer.onNext(balance);
                    }
                  } catch (JsonProcessingException e) {
                    e.printStackTrace();
                  } catch (IOException e) {
                    e.printStackTrace();
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
}
