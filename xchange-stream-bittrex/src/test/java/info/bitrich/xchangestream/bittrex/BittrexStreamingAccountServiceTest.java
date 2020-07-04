package info.bitrich.xchangestream.bittrex;

import io.reactivex.disposables.Disposable;
import org.junit.Assert;
import org.junit.Test;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw;
import org.knowm.xchange.bittrex.service.BittrexTradeService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

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
    BittrexTradeService bittrexTradeService = new BittrexTradeService(this.exchange);
    BittrexMarketDataService marketDataService = new BittrexMarketDataService(this.exchange);
    BittrexAccountService accountService = new BittrexAccountService(this.exchange);

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
}
