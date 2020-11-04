package info.bitrich.xchangestream.bittrex.services.account;

import info.bitrich.xchangestream.bittrex.BittrexStreamingAbstractService;
import info.bitrich.xchangestream.bittrex.BittrexStreamingService;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.bittrex.service.BittrexAccountServiceRaw;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static info.bitrich.xchangestream.bittrex.BittrexStreamingUtils.bittrexBalanceToBalance;

public class BittrexStreamingBalancesService
    extends BittrexStreamingAbstractService<BittrexBalance> {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingBalancesService.class);

  private final BittrexAccountService bittrexAccountService;
  private AtomicInteger currentSequenceNumber;
  private final ConcurrentMap<Currency, Subject<Balance>> balances;
  private final SortedSet<BittrexBalance> balancesUpdatesQueue;
  private final Object balancesLock;
  private AtomicInteger lastReceivedDeltaSequence;

  public BittrexStreamingBalancesService(
      BittrexStreamingService bittrexStreamingService,
      BittrexAccountService bittrexAccountService) {
    this.bittrexStreamingService = bittrexStreamingService;
    this.bittrexAccountService = bittrexAccountService;
    this.currentSequenceNumber = new AtomicInteger(-1);
    this.balances = new ConcurrentHashMap<>();
    this.balancesLock = new Object();
    this.balancesUpdatesQueue = new ConcurrentSkipListSet<>();
    this.lastReceivedDeltaSequence = null;
    this.messageHandler = createMessageHandler(BittrexBalance.class);
  }

  public Observable<Balance> getBalanceChanges(Currency currency) {
    subscribeToDataStream("balance", new String[] {"balance"}, true);
    if (!balances.containsKey(currency)) {
      initializeData(null);
    }
    return balances.get(currency);
  }

  @Override
  protected boolean isAccepted(BittrexBalance bittrexEntity) {
    return true;
  }

  @Override
  protected Number getLastReceivedSequence(BittrexBalance bittrexBalance) {
    return lastReceivedDeltaSequence;
  }

  @Override
  protected SortedSet<BittrexBalance> getUpdatesQueue(BittrexBalance bittrexBalance) {
    return balancesUpdatesQueue;
  }

  @Override
  protected void initializeData(BittrexBalance bittrexBalance) {
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
        initializeData(bittrexBalance);
      }
    }
  }

  @Override
  protected void queueUpdate(BittrexBalance bittrexBalance) {
    queueUpdate(balancesUpdatesQueue, bittrexBalance);
  }

  @Override
  protected void applyUpdates(BittrexBalance bittrexBalance) {
    if (balancesUpdatesQueue.first().getSequence() - currentSequenceNumber.get() > 1) {
      initializeData(bittrexBalance);
    } else {
      balancesUpdatesQueue.removeIf(update -> update.getSequence() <= currentSequenceNumber.get());
      balancesUpdatesQueue.forEach(
          balance -> {
            balances
                .get(balance.getDelta().getCurrencySymbol())
                .onNext(bittrexBalanceToBalance(balance));
            currentSequenceNumber = new AtomicInteger(balance.getSequence());
          });
    }
    balancesUpdatesQueue.clear();
  }

  @Override
  protected void updateLastReceivedSequence(BittrexBalance bittrexBalance) {
    lastReceivedDeltaSequence = new AtomicInteger(bittrexBalance.getSequence());
  }
}
