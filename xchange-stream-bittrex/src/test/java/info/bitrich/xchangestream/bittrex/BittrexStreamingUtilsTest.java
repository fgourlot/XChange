package info.bitrich.xchangestream.bittrex;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;

import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;
import junit.framework.TestCase;

public class BittrexStreamingUtilsTest extends TestCase {

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

}