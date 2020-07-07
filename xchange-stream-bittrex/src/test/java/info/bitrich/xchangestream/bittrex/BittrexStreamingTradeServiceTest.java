package info.bitrich.xchangestream.bittrex;

import io.reactivex.disposables.Disposable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataServiceRaw;
import org.knowm.xchange.bittrex.service.BittrexTradeService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingTradeServiceTest extends BittrexStreamingBaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingTradeServiceTest.class);

  /** Test order id for assertion */
  String limitOrderId;

  @Test
  public void openOrdersAfterOrderTest() {

    // init services
    BittrexTradeService bittrexTradeService = new BittrexTradeService((BittrexExchange) exchange);
    BittrexMarketDataService marketDataService =
        new BittrexMarketDataService((BittrexExchange) this.exchange);

    // test order params
    CurrencyPair currencyPair = CurrencyPair.ETH_BTC;
    Order.OrderType orderType = Order.OrderType.BID;
    BigDecimal tradeAmount = new BigDecimal(0.1);
    BigDecimal tradePrice = new BigDecimal(0);

    // get last bid order price from REST OrderBook
    try {
      BittrexMarketDataServiceRaw.SequencedOrderBook sequencedOrderBook =
          marketDataService.getBittrexSequencedOrderBook(
              BittrexUtils.toPairString(currencyPair), 500);
      OrderBook orderBook = sequencedOrderBook.getOrderBook();
      List<LimitOrder> bidOrders = orderBook.getBids();
      LimitOrder lastBidOrder = bidOrders.get(bidOrders.size() - 1);
      tradePrice = lastBidOrder.getLimitPrice();
    } catch (IOException e) {
      e.printStackTrace();
    }

    Disposable wsDisposable =
        exchange
            .getStreamingTradeService()
            .getUserTrades(CurrencyPair.ETH_BTC)
            .subscribe(
                order -> {
                  LOG.debug("Received order : {}", order);
                  Assert.assertTrue(order.getOrderId() == limitOrderId);
                });
    try {
      // forge and execute test order
      LimitOrder limitOrder =
          new LimitOrder.Builder(orderType, currencyPair)
              .limitPrice(tradePrice)
              .originalAmount(tradeAmount)
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
