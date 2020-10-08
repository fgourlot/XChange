package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrder;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;
import org.knowm.xchange.bittrex.BittrexConstants;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.account.Balance;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for the bittrex streaming. */
public final class BittrexStreamingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingUtils.class);

  private BittrexStreamingUtils() {
    // Utility class
  }

  /**
   * Clone orders.
   *
   * @param orders the orders to clone
   * @return the cloned orders
   */
  public static Stream<LimitOrder> cloneOrders(Collection<LimitOrder> orders) {
    return orders.stream().map(order -> LimitOrder.Builder.from(order).build());
  }

  /**
   * Update a given OrderBook with Bittrex deltas in a BittrexOrderBook message.
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
   * Creates an OrderType (ASK/BID) from an order direction String (`SELL`/`BUY`).
   *
   * @param orderDirection the order direction in Bittrex format
   * @return the converted order type
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

  /**
   * Creates an UserTrade object from a BittrexOrder object.
   *
   * @param bittrexOrder the order in Bittrex format
   * @return the bittrex order converted to an UserTrade
   */
  public static Order bittrexOrderToOrder(BittrexOrder bittrexOrder) {
    // build and return UserTrade
    return new LimitOrder.Builder(
            BittrexStreamingUtils.orderDirectionToOrderType(bittrexOrder.getDelta().getDirection()),
            BittrexUtils.toCurrencyPair(bittrexOrder.getDelta().getMarketSymbol()))
        .id(bittrexOrder.getDelta().getId())
        .averagePrice(bittrexOrder.getDelta().getLimit())
        .limitPrice(bittrexOrder.getDelta().getLimit())
        .originalAmount(bittrexOrder.getDelta().getQuantity())
        .timestamp(bittrexOrder.getDelta().getCreatedAt())
        .fee(bittrexOrder.getDelta().getCommission())
        .orderStatus(
            BittrexConstants.CLOSED.equals(bittrexOrder.getDelta().getStatus())
                ? Order.OrderStatus.CANCELED
                : Order.OrderStatus.NEW)
        .build();
  }

  /**
   * Creates a BittrexOrder object from a Bittrex `order` message.
   *
   * @param bittrexOrderMessage the Bittrex order message
   * @return the converted BittrexOrder pojo
   */
  public static BittrexOrder bittrexOrderMessageToBittrexOrder(
      String bittrexOrderMessage, ObjectMapper objectMapper) {
    try {
      // decompress message
      byte[] decompressedMessage = BittrexEncryptionUtils.decompress(bittrexOrderMessage);
      // parse JSON to Object
      return objectMapper.reader().readValue(decompressedMessage, BittrexOrder.class);
    } catch (IOException e) {
      LOG.error("Error converting Bittrex order message.", e);
    }
    return null;
  }

  /**
   * Creates a Balance object from a BittrexBalance object.
   *
   * @param bittrexBalance the BittrexBalance
   * @return the converted Balance pojo
   */
  public static Balance bittrexBalanceToBalance(BittrexBalance bittrexBalance) {
    return new Balance.Builder()
        .currency(bittrexBalance.getDelta().getCurrencySymbol())
        .total(bittrexBalance.getDelta().getTotal())
        .available(bittrexBalance.getDelta().getAvailable())
        .timestamp(bittrexBalance.getDelta().getUpdatedAt())
        .build();
  }

  /**
   * Creates a BittrexBalance object from a Bittrex `balance` message.
   *
   * @param bittrexBalanceMessage the Bittrex balance message
   * @return the converted BittrexBalance pojo
   */
  public static BittrexBalance bittrexBalanceMessageToBittrexBalance(
      String bittrexBalanceMessage, ObjectReader objectMapper) {
    try {
      // decompress message
      byte[] decompressedMessage = BittrexEncryptionUtils.decompress(bittrexBalanceMessage);
      // parse JSON to Object
      return objectMapper.readValue(decompressedMessage, BittrexBalance.class);
    } catch (IOException e) {
      LOG.error("Error converting Bittrex balance message.", e);
    }
    return null;
  }
}
