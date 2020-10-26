package info.bitrich.xchangestream.bittrex;

import static org.knowm.xchange.bittrex.BittrexConstants.CLOSED;

import com.fasterxml.jackson.databind.ObjectReader;
import info.bitrich.xchangestream.bittrex.dto.*;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.stream.Stream;
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
  public static LimitOrder bittrexOrderToOrder(BittrexOrder bittrexOrder) {
    // build and return UserTrade

    BittrexOrderDelta btxOrder = bittrexOrder.getDelta();

    Order.OrderStatus orderStatus;
    if (btxOrder.getQuantity().compareTo(btxOrder.getFillQuantity()) == 0) {
      orderStatus = Order.OrderStatus.FILLED;
    } else if (BigDecimal.ZERO.compareTo(btxOrder.getFillQuantity()) < 0) {
      if (CLOSED.equals(btxOrder.getStatus())) {
        orderStatus = Order.OrderStatus.CANCELED;
      } else {
        orderStatus = Order.OrderStatus.PARTIALLY_FILLED;
      }
    } else if (BigDecimal.ZERO.compareTo(btxOrder.getFillQuantity()) == 0) {
      if (CLOSED.equals(btxOrder.getStatus())) {
        orderStatus = Order.OrderStatus.CANCELED;
      } else {
        orderStatus = Order.OrderStatus.NEW;
      }
    } else {
      orderStatus = Order.OrderStatus.UNKNOWN;
    }

    return new LimitOrder.Builder(
            BittrexStreamingUtils.orderDirectionToOrderType(btxOrder.getDirection()),
            BittrexUtils.toCurrencyPair(btxOrder.getMarketSymbol()))
        .id(btxOrder.getId())
        .limitPrice(btxOrder.getLimit())
        .originalAmount(btxOrder.getQuantity())
        .remainingAmount(btxOrder.getQuantity().subtract(btxOrder.getFillQuantity()))
        .timestamp(btxOrder.getCreatedAt())
        .fee(btxOrder.getCommission())
        .orderStatus(orderStatus)
        .build();
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
   * Clones an orderbook.
   *
   * @param orderBook the order book to clone
   * @return the cloned order book
   */
  public static OrderBook cloneOrderBook(OrderBook orderBook) {
    return new OrderBook(
        Optional.ofNullable(orderBook.getTimeStamp())
            .map(Date::getTime)
            .map(Date::new)
            .orElse(null),
        BittrexStreamingUtils.cloneOrders(orderBook.getAsks()),
        BittrexStreamingUtils.cloneOrders(orderBook.getBids()));
  }

  /**
   * Creates a BittrexEntity object from a Bittrex message.
   *
   * @param bittrexMessage the input Bittrex message
   * @param reader the message reader
   * @param bittrexClass the output bittrex class
   * @return the converted bittrex entity
   */
  public static <T> Optional<T> extractBittrexEntity(
      String bittrexMessage, ObjectReader reader, Class<T> bittrexClass) {
    try {
      byte[] decompressedMessage = BittrexEncryptionUtils.decompress(bittrexMessage);
      return Optional.of(reader.readValue(decompressedMessage, bittrexClass))
          .map(bittrexClass::cast);
    } catch (IOException e) {
      LOG.error("Error extracting {} message! {}", bittrexClass.getSimpleName(), e);
    }
    return Optional.empty();
  }
}
