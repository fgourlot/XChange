package info.bitrich.xchangestream.bittrex;

import java.io.IOException;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.bittrex.service.BittrexAccountServiceRaw;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscription;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscriptionHandler;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.core.StreamingAccountService;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;

public class BittrexStreamingAccountService implements StreamingAccountService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAccountService.class);

  private static final int MAX_DELTAS_IN_MEMORY = 100_000;

  private final BittrexStreamingService bittrexStreamingService;
  private final BittrexAccountService bittrexAccountService;

  /** Current sequence number (to be increased after each message) */
  private AtomicInteger currentSequenceNumber;

  private final BittrexStreamingSubscriptionHandler balancesMessageHandler;
  private final ObjectMapper objectMapper;
  private boolean isBalancesChannelSubscribed;
  private final ConcurrentMap<Currency, Subject<Balance>> balances;
  private final SortedSet<BittrexBalance> balancesDeltaQueue;
  private final Object subscribeLock = new Object();
  private final Object balancesLock = new Object();
  private AtomicInteger lastReceivedDeltaSequence;

  public BittrexStreamingAccountService(
      BittrexStreamingService bittrexStreamingService,
      BittrexAccountService bittrexAccountService) {
    this.bittrexStreamingService = bittrexStreamingService;
    this.bittrexAccountService = bittrexAccountService;
    this.currentSequenceNumber = new AtomicInteger(-1);
    this.balances = new ConcurrentHashMap<>();
    this.balancesDeltaQueue = new ConcurrentSkipListSet<>();
    this.objectMapper = new ObjectMapper();
    this.lastReceivedDeltaSequence = null;
    this.balancesMessageHandler = createBalancesMessageHandler();
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
      initializeBalances();
    }
    return balances.get(currency);
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
            if (!isSequenceValid(bittrexBalance.getSequence())) {
              balancesDeltaQueue.clear();
            } else {
              queueBalanceDelta(bittrexBalance);
              updateBalances();
            }
          }
        });
  }

  private boolean isSequenceValid(int sequence) {
    boolean isValid =
        lastReceivedDeltaSequence == null || lastReceivedDeltaSequence.get() + 1 == sequence;
    lastReceivedDeltaSequence = new AtomicInteger(sequence);
    return isValid;
  }

  private void updateBalances() {
    balancesDeltaQueue.removeIf(delta -> delta.getSequence() <= currentSequenceNumber.get());
    if (!balancesDeltaQueue.isEmpty()) {
      if (balancesDeltaQueue.stream()
          .map(BittrexBalance::getSequence)
          .noneMatch(sequence -> sequence == currentSequenceNumber.get() + 1)) {
        LOG.info("Balances desync! Sequences to apply: {}, last is {}", balancesDeltaQueue, currentSequenceNumber);
        initializeBalances();
      }
      balancesDeltaQueue.forEach(
          bittrexBalance -> {
            balances
                .get(bittrexBalance.getDelta().getCurrencySymbol())
                .onNext(BittrexStreamingUtils.bittrexBalanceToBalance(bittrexBalance));
            currentSequenceNumber = new AtomicInteger(bittrexBalance.getSequence());
          });
      balancesDeltaQueue.clear();
    }
  }

  private void queueBalanceDelta(BittrexBalance bittrexBalance) {
    balancesDeltaQueue.add(bittrexBalance);
    while (balancesDeltaQueue.size() > MAX_DELTAS_IN_MEMORY) {
      balancesDeltaQueue.remove(balancesDeltaQueue.first());
    }
  }

  /** Subscribes to all of the order books channels available via getting ticker in one go. */
  private void subscribeToBalancesChannels() {
    String balanceChannel = "balance";

    BittrexStreamingSubscription subscription =
        new BittrexStreamingSubscription(
            "balance", new String[] {balanceChannel}, true, this.balancesMessageHandler);
    bittrexStreamingService.subscribeToChannelWithHandler(subscription);
    isBalancesChannelSubscribed = true;
  }

  private void initializeBalances() {
    synchronized (balancesLock) {
      try {
        LOG.info("Initializing balances with rest");
        BittrexAccountServiceRaw.SequencedBalances sequencedBalances =
            bittrexAccountService.getBittrexSequencedBalances();
        sequencedBalances
            .getBalances()
            .values()
            .forEach(
                balance -> {
                  if (balances.containsKey(balance.getCurrency())) {
                    balances.get(balance.getCurrency()).onNext(balance);
                  } else {
                    balances.put(
                        balance.getCurrency(),
                        BehaviorSubject.createDefault(balance).toSerialized());
                  }
                });
        currentSequenceNumber =
            new AtomicInteger(Integer.parseInt(sequencedBalances.getSequence()));
      } catch (IOException e) {
        LOG.error("Error rest fetching balances", e);
        initializeBalances();
      }
    }
  }
}
