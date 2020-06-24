package info.bitrich.xchangestream.bittrex;

import io.reactivex.disposables.Disposable;
import org.junit.Assert;
import org.junit.Test;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw;
import org.knowm.xchange.bittrex.service.BittrexTradeService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.UserTrade;
import org.knowm.xchange.dto.trade.UserTrades;
import org.knowm.xchange.service.trade.params.DefaultTradeHistoryParamCurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

public class BittrexStreamingAccountServiceTest extends BittrexStreamingBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingAccountServiceTest.class);
  String limitOrderId;

  @Test
  public void balanceAfterOrderTest() {

    BittrexTradeService bittrexTradeService = new BittrexTradeService(exchange);
    BittrexMarketDataService marketDataService = new BittrexMarketDataService(this.exchange);

    // test order params
    CurrencyPair currencyPair = CurrencyPair.ETH_BTC;
    Order.OrderType orderType = Order.OrderType.BID;
    BigDecimal tradeAmount = new BigDecimal(0.1);
    BigDecimal tradePrice = new BigDecimal(0);

    // get last bid order price from REST OrderBook
    try {
      BittrexMarketDataServiceRaw.SequencedOrderBook sequencedOrderBook =
          marketDataService.getBittrexSequencedOrderBook(
              BittrexUtils.toPairString(currencyPair, true), 500);
      OrderBook orderBook = sequencedOrderBook.getOrderBook();
      List<LimitOrder> bidOrders = orderBook.getBids();
      LimitOrder lastBidOrder = bidOrders.get(bidOrders.size() - 1);
      tradePrice = lastBidOrder.getLimitPrice();
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
                  // get trade history
                  DefaultTradeHistoryParamCurrencyPair tradeHistoryParams =
                      new DefaultTradeHistoryParamCurrencyPair(currencyPair);
                  UserTrades userTrades = bittrexTradeService.getTradeHistory(tradeHistoryParams);
                  // find and assert test order
                  Assert.assertTrue(
                      userTrades.getTrades().contains(new UserTrade.Builder().id(limitOrderId)));
                });

    try {
      // forge and execute test order
      LimitOrder limitOrder =
          new LimitOrder.Builder(orderType, currencyPair)
              .limitPrice(tradePrice)
              .originalAmount(tradeAmount)
              .build();
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
