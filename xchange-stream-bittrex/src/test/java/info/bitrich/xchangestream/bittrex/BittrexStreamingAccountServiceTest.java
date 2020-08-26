package info.bitrich.xchangestream.bittrex;

import io.reactivex.disposables.Disposable;
import org.junit.Assert;
import org.junit.Test;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw;
import org.knowm.xchange.bittrex.service.BittrexTradeService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.account.Balance;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class BittrexStreamingAccountServiceTest extends BittrexStreamingBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingAccountServiceTest.class);

  /** Test order id for assertion */
  String limitOrderId;

  BigDecimal availableCurrencyBeforeOrder;
  BigDecimal orderCostWithFees;

  @Test
  public void balanceAfterOrderTest() {

    // init services
    BittrexTradeService bittrexTradeService =
        new BittrexTradeService((BittrexExchange) this.exchange);
    BittrexMarketDataService marketDataService =
        new BittrexMarketDataService((BittrexExchange) this.exchange);
    BittrexAccountService accountService =
        new BittrexAccountService((BittrexExchange) this.exchange);

    // test order params
    CurrencyPair currencyPair = CurrencyPair.ETH_BTC;
    Order.OrderType orderType = Order.OrderType.BID;
    BigDecimal orderAmount = new BigDecimal(0.1);
    BigDecimal orderPrice = new BigDecimal(0);

    // get last bid order price from REST OrderBook
    try {
      BittrexMarketDataServiceRaw.SequencedOrderBook sequencedOrderBook =
          marketDataService.getBittrexSequencedOrderBook(
              BittrexUtils.toPairString(currencyPair), 500);
      OrderBook orderBook = sequencedOrderBook.getOrderBook();
      List<LimitOrder> bidOrders = orderBook.getBids();
      LimitOrder lastBidOrder = bidOrders.get(bidOrders.size() - 1);
      orderPrice = lastBidOrder.getLimitPrice();
      // calculate order cost
      BigDecimal fees = new BigDecimal("1.002");
      BigDecimal orderCost = orderPrice.multiply(orderAmount);
      orderCostWithFees = orderCost.multiply(fees);
      // get available currency
      availableCurrencyBeforeOrder = accountService.getBittrexBalance(Currency.BTC).getAvailable();
      LOG.info("available before trade {}", availableCurrencyBeforeOrder);
    } catch (IOException e) {
      e.printStackTrace();
    }

    limitOrderId = null;

    Disposable wsDisposable =
        exchange
            .getStreamingAccountService()
            .getBalanceChanges(Currency.BTC)
            .subscribe(
                balance -> {
                  LOG.debug("Received balance : {}", balance);
                  BigDecimal newBalance = availableCurrencyBeforeOrder.subtract(orderCostWithFees);
                  Assert.assertTrue(balance.getAvailable().compareTo(newBalance) == 0);
                });

    try {
      // forge and execute test order
      LimitOrder limitOrder =
          new LimitOrder.Builder(orderType, currencyPair)
              .limitPrice(orderPrice)
              .originalAmount(orderAmount)
              .build();
      limitOrderId = bittrexTradeService.placeLimitOrder(limitOrder);
      LOG.info("Performed order with id : {}", limitOrderId);
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      Thread.sleep(7000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      // Stopping everyone
      wsDisposable.dispose();
    }
  }

  @Test
  public void testBalances() throws InterruptedException, IOException {
    BittrexAccountService accountService =
        new BittrexAccountService((BittrexExchange) this.exchange);


    List<Map<Currency, Balance>> balancesStreamList = new ArrayList<>();
    ConcurrentMap<Currency, Balance> currentMapStream = new ConcurrentHashMap<>();

    Map<Currency, Balance> balances = accountService.getAccountInfo().getWallet().getBalances();
    List<Disposable> subs = new ArrayList<>();
    final Object streamLock = new Object();
    balances
        .keySet()
        .forEach(
            currency -> {
              Disposable wsDisposable =
                  exchange
                      .getStreamingAccountService()
                      .getBalanceChanges(currency)
                      .subscribe(
                          balance -> {
                            synchronized (streamLock) {
                              currentMapStream.put(currency, balance);
                              Map<Currency, Balance> clonedBalances = cloneMap(currentMapStream);
                              balancesStreamList.add(clonedBalances);
                            }
                          });
              subs.add(wsDisposable);
            });

    Thread.sleep(5_000);

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

    Thread.sleep(20_000);
    timer.cancel();
    Thread.sleep(5_000);
    subs.forEach(Disposable::dispose);

    Thread.sleep(5_000);
    if (balancesStreamList.size() > 0) {
      LOG.debug("Found balance messages in stream list : {}", balancesStreamList);
      balancesStreamList.forEach(
          currencyBalanceMap -> {
            Map<Currency, Balance> restEntry = balancesMapsRest.get(0);
            currencyBalanceMap.forEach(
                (currency, balance) -> {
                  if (restEntry.containsKey(currency)) {
                    LOG.error("Currency balance from stream found in REST API");
                  } else {
                    LOG.error("Currency balance from stream list not found in REST API");
                    Assert.fail();
                  }
                });
          });
        // at this point, no fail means success
        Assert.assertTrue(true);
    } else {
      LOG.error("no balance message in stream list");
      Assert.fail();
    }
    /* this can't work
    balancesMapsRest.forEach(entry -> {
      currencyEntry = entry.get()
      Assert.assertTrue(balancesStreamList.contains(entry));
    });*/
  }

  private Map<Currency, Balance> cloneMap(Map<Currency, Balance> balancesToClone) {
    Map<Currency, Balance> clonedMap = new HashMap<>(balancesToClone.size());
    balancesToClone
        .forEach((key, value) -> {
          Balance clonedBalance = Balance.Builder.from(value).build();
          clonedMap.put(key, clonedBalance);
        });
    return clonedMap;
  }
}
