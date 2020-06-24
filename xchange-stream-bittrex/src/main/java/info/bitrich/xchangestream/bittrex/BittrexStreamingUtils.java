package info.bitrich.xchangestream.bittrex;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;

import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;

/** Utility class for the bittrex streaming. */
public final class BittrexStreamingUtils {

  private BittrexStreamingUtils() {
    // Utility class
  }

  /**
   * Clone orders
   *
   * @param orders the orders to clone
   * @return the cloned orders
   */
  public static Stream<LimitOrder> cloneOrders(Collection<LimitOrder> orders) {
    return orders.stream().map(order -> LimitOrder.Builder.from(order).build());
  }

  /**
   * Update a given OrderBook with Bittrex deltas in a BittrexOrderBook message
   *
   * @param orderBookToUpdate the order book to update
   * @param updates the updates to apply
   * @return the updated order book
   */
  public static OrderBook updateOrderBook(
      OrderBook orderBookToUpdate, BittrexOrderBookDeltas updates) {
    CurrencyPair market = BittrexUtils.toCurrencyPair(updates.getMarketSymbol());
    applyOrderBookUpdates(orderBookToUpdate, updates.getAskDeltas(), Order.OrderType.ASK, market);
    applyOrderBookUpdates(orderBookToUpdate, updates.getBidDeltas(), Order.OrderType.BID, market);
    return orderBookToUpdate;
  }

  /**
   * Apply updates to an order book.
   *
   * @param orderBookToUpdate the order book to update
   * @param updates the updates to apply
   * @param orderType the order book side to update (bids or asks)
   * @param market the market name
   */
  public static void applyOrderBookUpdates(
      OrderBook orderBookToUpdate,
      BittrexOrderBookEntry[] updates,
      Order.OrderType orderType,
      CurrencyPair market) {
    Arrays.stream(updates)
        .map(
            update ->
                new LimitOrder.Builder(orderType, market)
                    .originalAmount(update.getQuantity())
                    .limitPrice(update.getRate())
                    .build())
        .forEach(orderBookToUpdate::update);
  }

  /**
   * Creates an OrderType (ASK/BID) from an order direction String (`SELL`/`BUY`)
   * @param orderDirection
   * @return
   */
  public static Order.OrderType orderDirectionToOrderType(String orderDirection) {
    switch (orderDirection.toUpperCase()) {
      case "BUY":
        return Order.OrderType.BID;
      case "SELL":
        return Order.OrderType.ASK;
      default:
        return null;
    }
  }
}
