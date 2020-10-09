package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscription;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscriptionHandler;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.core.StreamingAccountService;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;
import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.bittrex.service.BittrexAccountServiceRaw;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingAccountService implements StreamingAccountService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAccountService.class);

  private static final int MAX_DELTAS_IN_MEMORY = 100_000;

  private final BittrexStreamingService bittrexStreamingService;
  private final BittrexAccountService bittrexAccountService;

  /** Current sequence number (to be increased after each message) */
  private Integer currentSequenceNumber;

  private final BittrexStreamingSubscriptionHandler balancesMessageHandler;
  private final ObjectMapper objectMapper;
  private boolean isBalancesChannelSubscribed;
  private final Map<Currency, Subject<Balance>> balances;
  private final SortedSet<BittrexBalance> balancesDeltaQueue;
  private final Object subscribeLock = new Object();
  private final Object balancesLock = new Object();

  public BittrexStreamingAccountService(
      BittrexStreamingService bittrexStreamingService,
      BittrexAccountService bittrexAccountService) {
    this.bittrexStreamingService = bittrexStreamingService;
    this.bittrexAccountService = bittrexAccountService;
    this.balances = new ConcurrentHashMap<>();
    this.balancesDeltaQueue = new ConcurrentSkipListSet<>();
    this.objectMapper = new ObjectMapper();
    this.balancesMessageHandler = createBalancesMessageHandler();
  }

  /**
   * Creates the handler which will work with the websocket incoming messages.
   *
   * @return the created handler
   */
  private BittrexStreamingSubscriptionHandler createBalancesMessageHandler() {
    return new BittrexStreamingSubscriptionHandler(
        message -> {
          BittrexBalance bittrexBalance =
              BittrexStreamingUtils.bittrexBalanceMessageToBittrexBalance(
                  message, objectMapper.reader());
          if (bittrexBalance != null) {
            queueBalanceDelta(bittrexBalance);
            synchronized (balancesLock) {
              if (needBalancesInit(bittrexBalance)) {
                restFillBalances();
              }
              applyBalancesDeltas();
            }
          }
        });
  }

  private void applyBalancesDeltas() {
    balancesDeltaQueue.stream()
        .filter(bittrexBalance -> bittrexBalance.getSequence() > currentSequenceNumber)
        .forEach(
            bittrexBalance -> {
              balances
                  .get(bittrexBalance.getDelta().getCurrencySymbol())
                  .onNext(BittrexStreamingUtils.bittrexBalanceToBalance(bittrexBalance));
              currentSequenceNumber = bittrexBalance.getSequence();
            });
    balancesDeltaQueue.clear();
  }

  private void queueBalanceDelta(BittrexBalance bittrexBalance) {
    balancesDeltaQueue.add(bittrexBalance);
    if (balancesDeltaQueue.size() > MAX_DELTAS_IN_MEMORY) {
      balancesDeltaQueue.remove(balancesDeltaQueue.first());
    }
  }

  @Override
  public Observable<Balance> getBalanceChanges(Currency currency, Object... args) {
    if (!isBalancesChannelSubscribed) {
      synchronized (subscribeLock) {
        if (!isBalancesChannelSubscribed) {
          subscribeToBalancesChannels();
        }
      }
    }
    if (!balances.containsKey(currency)) {
      restFillBalances();
    }
    return balances.get(currency);
  }

  /** Subscribes to all of the order books channels available via getting ticker in one go. */
  private void subscribeToBalancesChannels() {
    String balanceChannel = "balance";

    BittrexStreamingSubscription subscription =
        new BittrexStreamingSubscription(
            "balance", new String[] {balanceChannel}, this.balancesMessageHandler);
    bittrexStreamingService.subscribeToChannelWithHandler(subscription, true);
    isBalancesChannelSubscribed = true;
  }

  private boolean needBalancesInit(BittrexBalance bittrexBalance) {
    return currentSequenceNumber + 1 < bittrexBalance.getSequence()
        || currentSequenceNumber == null;
  }

  private void restFillBalances() {
    synchronized (balancesLock) {
      try {
        BittrexAccountServiceRaw.SequencedBalances sequencedBalances =
            bittrexAccountService.getBittrexSequencedBalances();
        balances.clear();
        balances.putAll(
            sequencedBalances.getBalances().values().stream()
                .collect(
                    Collectors.toMap(
                        Balance::getCurrency,
                        balance -> BehaviorSubject.createDefault(balance).toSerialized())));
        currentSequenceNumber = Integer.parseInt(sequencedBalances.getSequence());
      } catch (IOException e) {
        LOG.error("Error rest fetching balances", e);
      }
    }
  }
}
