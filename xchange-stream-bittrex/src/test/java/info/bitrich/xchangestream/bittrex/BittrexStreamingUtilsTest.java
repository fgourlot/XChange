package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrder;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.account.Balance;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.UserTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class BittrexStreamingUtilsTest extends TestCase {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingUtilsTest.class);

  public void testUpdateOrderBook() {
    CurrencyPair market = CurrencyPair.ETH_BTC;
    String sequence = "1234";

    // Orderbook to update
    List<LimitOrder> bookBids = new ArrayList<>();
    List<LimitOrder> bookAsks = new ArrayList<>();

    LimitOrder bid1 =
        new LimitOrder(
            Order.OrderType.BID, new BigDecimal("1"), market, null, null, new BigDecimal("5"));
    LimitOrder bid2 =
        new LimitOrder(
            Order.OrderType.BID, new BigDecimal("2"), market, null, null, new BigDecimal("4"));
    LimitOrder bid3 =
        new LimitOrder(
            Order.OrderType.BID, new BigDecimal("3"), market, null, null, new BigDecimal("3"));
    bookBids.add(bid1);
    bookBids.add(bid2);
    bookBids.add(bid3);

    LimitOrder ask1 =
        new LimitOrder(
            Order.OrderType.ASK, new BigDecimal("1"), market, null, null, new BigDecimal("6"));
    LimitOrder ask2 =
        new LimitOrder(
            Order.OrderType.ASK, new BigDecimal("2"), market, null, null, new BigDecimal("7"));
    LimitOrder ask3 =
        new LimitOrder(
            Order.OrderType.ASK, new BigDecimal("3"), market, null, null, new BigDecimal("8"));
    bookAsks.add(ask1);
    bookAsks.add(ask2);
    bookAsks.add(ask3);

    OrderBook orderBook = new OrderBook(null, bookAsks, bookBids, true);

    // delta entries to apply to the orderbook to update
    BittrexOrderBookEntry deleteEntry =
        new BittrexOrderBookEntry(BigDecimal.ZERO, new BigDecimal("4"));
    BittrexOrderBookEntry deleteEntry2 =
        new BittrexOrderBookEntry(BigDecimal.ZERO, new BigDecimal("7"));
    BittrexOrderBookEntry deleteEntry3 =
        new BittrexOrderBookEntry(BigDecimal.ZERO, new BigDecimal("6"));
    BittrexOrderBookEntry addEntry =
        new BittrexOrderBookEntry(new BigDecimal("1.1"), new BigDecimal("7.5"));
    BittrexOrderBookEntry addEntry2 =
        new BittrexOrderBookEntry(new BigDecimal("1.2"), new BigDecimal("7.4"));

    BittrexOrderBookEntry[] deltaBidsEntries = {deleteEntry};
    BittrexOrderBookEntry[] deltaAsksEntries = {deleteEntry3, addEntry, addEntry2, deleteEntry2};

    BittrexOrderBookDeltas bittrexOrderBookDeltas =
        new BittrexOrderBookDeltas(
            "ETH-BTC", 500, Integer.parseInt(sequence), deltaAsksEntries, deltaBidsEntries);

    // apply the updates
    OrderBook orderBookUpdated =
        BittrexStreamingUtils.updateOrderBook(orderBook, bittrexOrderBookDeltas);

    // Expected Orderbook
    List<LimitOrder> updatedBookBids = new ArrayList<>();
    List<LimitOrder> updatedBookAsks = new ArrayList<>();

    LimitOrder expectedbid1 =
        new LimitOrder(
            Order.OrderType.BID, new BigDecimal("1"), market, null, null, new BigDecimal("5"));
    LimitOrder expectedbid2 =
        new LimitOrder(
            Order.OrderType.BID, new BigDecimal("3"), market, null, null, new BigDecimal("3"));
    updatedBookBids.add(expectedbid1);
    updatedBookBids.add(expectedbid2);

    // LimitOrder expectedask1 = new LimitOrder(Order.OrderType.ASK, new BigDecimal("1"), market,
    // null, null, new BigDecimal("6"));
    LimitOrder expectedask4 =
        new LimitOrder(
            Order.OrderType.ASK, new BigDecimal("1.2"), market, null, null, new BigDecimal("7.4"));
    LimitOrder expectedask3 =
        new LimitOrder(
            Order.OrderType.ASK, new BigDecimal("1.1"), market, null, null, new BigDecimal("7.5"));
    LimitOrder expectedask2 =
        new LimitOrder(
            Order.OrderType.ASK, new BigDecimal("3"), market, null, null, new BigDecimal("8"));
    // updatedBookAsks.add(expectedask1);
    updatedBookAsks.add(expectedask2);
    updatedBookAsks.add(expectedask3);
    updatedBookAsks.add(expectedask4);

    OrderBook expectedUpdatedOrderBook =
        new OrderBook(null, updatedBookAsks, updatedBookBids, true);

    // Test that the initial orderbook with deltas applied is equal to the expected orderbook
    Assert.assertTrue(orderBookUpdated.ordersEqual(expectedUpdatedOrderBook));
  }

  /** Test an encoded Bittrex order message to UserTrade object conversion */
  public void testBittrexOrderToUserTrade() {
    try {
      // read encoded order message (from mock)
      String orderMessage =
          IOUtils.toString(getClass().getResource("/orderMessage_encoded.txt"), "UTF8");
      BittrexOrder bittrexOrder =
          BittrexStreamingUtils.bittrexOrderMessageToBittrexOrder(orderMessage);
      UserTrade userTrade = BittrexStreamingUtils.bittrexOrderToUserTrade(bittrexOrder);
      assertEquals(userTrade.getOrderId(), "738a6aed-9d09-4035-92af-1dcae3872e52");
      assertEquals(userTrade.getCurrencyPair(), CurrencyPair.ETH_BTC);
      assertEquals(userTrade.getPrice(), new BigDecimal("0.02070000"));
      assertEquals(userTrade.getOriginalAmount(), new BigDecimal("0.10000000"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /** Test an encoded Bittrex balance message to Balance object conversion */
  public void testBittrexBalanceMessageToBalance() {
    try {
      // read encoded balance message (from mock)
      String balanceMessage =
          IOUtils.toString(getClass().getResource("/balanceMessage_encoded.txt"), "UTF8");
      BittrexBalance bittrexBalance =
          BittrexStreamingUtils.bittrexBalanceMessageToBittrexBalance(balanceMessage);
      Balance balance = BittrexStreamingUtils.bittrexBalanceToBalance(bittrexBalance);
      assertEquals(balance.getCurrency(), Currency.BTC);
      assertEquals(balance.getTotal(), new BigDecimal("0.00804302"));
      assertEquals(balance.getAvailable(), new BigDecimal("0.00596149"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
