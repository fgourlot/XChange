package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.core.StreamingAccountService;
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


/** See https://bittrex.github.io/api/v3#topic-Websocket-Overview */
public class BittrexStreamingAccountService extends BittrexStreamingAbstractService<BittrexBalance>
    implements StreamingAccountService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAccountService.class);

  private final BittrexAccountService bittrexAccountService;
  private AtomicInteger currentSequenceNumber;
  private final ConcurrentMap<Currency, Subject<Balance>> balances;
  private final SortedSet<BittrexBalance> balancesDeltaQueue;
  private final Object balancesLock;
  private AtomicInteger lastReceivedDeltaSequence;

  public BittrexStreamingAccountService(
      BittrexStreamingService bittrexStreamingService,
      BittrexAccountService bittrexAccountService) {
    this.bittrexStreamingService = bittrexStreamingService;
    this.bittrexAccountService = bittrexAccountService;
    this.currentSequenceNumber = new AtomicInteger(-1);
    this.balances = new ConcurrentHashMap<>();
    this.balancesLock = new Object();
    this.balancesDeltaQueue = new ConcurrentSkipListSet<>();
    this.lastReceivedDeltaSequence = null;
    this.messageHandler = createMessageHandler(BittrexBalance.class);
  }

  @Override
  public Observable<Balance> getBalanceChanges(Currency currency, Object... args) {
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
  protected SortedSet<BittrexBalance> getDeltaQueue(BittrexBalance bittrexBalance) {
    return balancesDeltaQueue;
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
  protected void queueDelta(BittrexBalance bittrexBalance) {
    balancesDeltaQueue.add(bittrexBalance);
    while (balancesDeltaQueue.size() > MAX_DELTAS_IN_MEMORY) {
      balancesDeltaQueue.remove(balancesDeltaQueue.first());
    }
  }

  @Override
  protected void updateData(BittrexBalance bittrexBalance) {
    if (balancesDeltaQueue.first().getSequence() - currentSequenceNumber.get() > 1) {
      initializeData(bittrexBalance);
    } else {
      balancesDeltaQueue.removeIf(delta -> delta.getSequence() <= currentSequenceNumber.get());
      balancesDeltaQueue.forEach(
          balance -> {
            balances
                .get(balance.getDelta().getCurrencySymbol())
                .onNext(BittrexStreamingUtils.bittrexBalanceToBalance(balance));
            currentSequenceNumber = new AtomicInteger(balance.getSequence());
          });
    }
    balancesDeltaQueue.clear();
  }

  @Override
  protected void updateLastReceivedSequence(BittrexBalance bittrexBalance) {
    lastReceivedDeltaSequence = new AtomicInteger(bittrexBalance.getSequence());
  }
}
