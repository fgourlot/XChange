package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.service.BittrexTradeService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.UserTrade;
import org.knowm.xchange.dto.trade.UserTrades;
import org.knowm.xchange.service.trade.params.DefaultTradeHistoryParamCurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.Timer;

public class BittrexStreamingAccountServiceTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingAccountServiceTest.class);
  static ExchangeSpecification exchangeSpecification;
  static CurrencyPair market = CurrencyPair.ETH_BTC;
  static StreamingExchange exchange;
  String limitOrderId;

  @BeforeClass
  public static void setup() {
    String apiKey = System.getProperty("apiKey");
    String apiSecret = System.getProperty("apiSecret");
    market = CurrencyPair.ETH_BTC;
    exchangeSpecification = new ExchangeSpecification(BittrexStreamingExchange.class.getName());
    exchangeSpecification.setApiKey(apiKey);
    exchangeSpecification.setSecretKey(apiSecret);
    exchange = StreamingExchangeFactory.INSTANCE.createExchange(exchangeSpecification);
    exchange.connect().blockingAwait();
  }

  @Test
  public void balanceAfterOrderTest() {
    // test order params
    Order.OrderType orderType = Order.OrderType.BID;
    CurrencyPair currencyPair = CurrencyPair.ETH_BTC;
    BigDecimal tradePrice = new BigDecimal(0.02513230); // TODO : find a way to not hard-code price
    BigDecimal tradeAmount = new BigDecimal(0.1);
    BittrexTradeService bittrexTradeService = new BittrexTradeService(exchange);
    limitOrderId = null;

    // forge test order
    LimitOrder limitOrder =
        new LimitOrder.Builder(orderType, currencyPair)
            .limitPrice(tradePrice)
            .originalAmount(tradeAmount)
            .build();

    Disposable wsDisposable =
        exchange
            .getStreamingAccountService()
            .getBalanceChanges(Currency.BTC)
            .subscribe(
                balance -> {
                  LOG.debug("Received balance : {}", balance);
                  // get trade history
                  DefaultTradeHistoryParamCurrencyPair tradeHistoryParams =
                      new DefaultTradeHistoryParamCurrencyPair(currencyPair);
                  UserTrades userTrades = bittrexTradeService.getTradeHistory(tradeHistoryParams);
                  // find and assert test order
                  Assert.assertTrue(
                      userTrades
                          .getTrades()
                          .contains(
                              new UserTrade.Builder()
                                  .price(tradePrice)
                                  .originalAmount(tradeAmount)
                                  .currencyPair(currencyPair)
                                  .id(limitOrderId)));
                });

    try {
      // execute order
      limitOrderId = bittrexTradeService.placeLimitOrder(limitOrder);
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
}
