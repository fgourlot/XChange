package info.bitrich.xchangestream.bittrex;

import io.reactivex.disposables.Disposable;
import org.junit.Assert;
import org.junit.Test;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BittrexStreamingAccountServiceTest extends BittrexStreamingBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingAccountServiceTest.class);

  @Test
  public void testBalances() throws InterruptedException, IOException {
    BittrexAccountService accountService =
        (BittrexAccountService) this.exchange.getAccountService();

    List<Map<Currency, Balance>> balancesStreamList = new ArrayList<>();
    ConcurrentMap<Currency, Balance> currentMapStream = new ConcurrentHashMap<>();

    Map<Currency, Balance> balances = accountService.getAccountInfo().getWallet().getBalances();
    List<Disposable> disposables = new ArrayList<>();
    final Object streamLock = new Object();
    balances
        .keySet()
        // Stream.of(Currency.BTC)
        .forEach(
            currency -> {
              Disposable wsDisposable =
                  exchange
                      .getStreamingAccountService()
                      .getBalanceChanges(currency)
                      .subscribe(
                          balance -> {
                            synchronized (streamLock) {
                              // if (balance.getCurrency().equals(Currency.USDT)) {
                              LOG.debug("Received balance update {}", balance);
                              // }
                              currentMapStream.put(currency, balance);
                              Map<Currency, Balance> clonedBalances = cloneMap(currentMapStream);
                              balancesStreamList.add(clonedBalances);
                            }
                          });
              disposables.add(wsDisposable);
            });
    while (balancesStreamList.get(balancesStreamList.size() - 1).size() < balances.size()) {
      Thread.sleep(100);
    }

    List<Map<Currency, Balance>> balancesMapsRest = new ArrayList<>();
    Timer timer = new Timer();
    timer.scheduleAtFixedRate(
        new TimerTask() {
          public void run() {
            try {
              balancesMapsRest.add(accountService.getAccountInfo().getWallet().getBalances());
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        },
        0,
        TimeUnit.SECONDS.toMillis(2));

    Thread.sleep(30_000);
    timer.cancel();
    Thread.sleep(5_000);
    disposables.forEach(Disposable::dispose);

    List<Integer> indexesFound =
        balancesMapsRest.stream().map(balancesStreamList::indexOf).collect(Collectors.toList());
    indexesFound.forEach(index -> LOG.info(index.toString()));
    Assert.assertTrue(indexesFound.stream().allMatch(index -> index > 0));
  }

  private Map<Currency, Balance> cloneMap(Map<Currency, Balance> balancesToClone) {
    Map<Currency, Balance> clonedMap = new HashMap<>(balancesToClone.size());
    balancesToClone.forEach(
        (key, value) -> {
          Balance clonedBalance = Balance.Builder.from(value).build();
          clonedMap.put(key, clonedBalance);
        });
    return clonedMap;
  }
}
