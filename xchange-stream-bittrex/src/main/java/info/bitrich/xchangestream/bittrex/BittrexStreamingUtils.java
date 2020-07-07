package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrder;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.account.Balance;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.UserTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for the bittrex streaming. */
public final class BittrexStreamingUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingUtils.class);

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
   *
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

  /**
   * Creates an UserTrade object from a BittrexOrder message
   *
   * @param bittrexOrder
   * @return
   */
  public static UserTrade bittrexOrderMessageToUserTrade(String bittrexOrderMessage) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      // decompress message
      String decompressedMessage = EncryptionUtils.decompress(bittrexOrderMessage);
      LOG.debug("Decompressed order message : {}", decompressedMessage);
      // parse JSON to Object
      BittrexOrder bittrexOrder = objectMapper.readValue(decompressedMessage, BittrexOrder.class);
      // build and return UserTrade
      return new UserTrade.Builder()
          .type(
              BittrexStreamingUtils.orderDirectionToOrderType(
                  bittrexOrder.getDelta().getDirection()))
          .currencyPair(BittrexUtils.toCurrencyPair(bittrexOrder.getDelta().getMarketSymbol()))
          .orderId(bittrexOrder.getDelta().getId())
          .price(bittrexOrder.getDelta().getLimit())
          .originalAmount(bittrexOrder.getDelta().getQuantity())
          .timestamp(bittrexOrder.getDelta().getCreatedAt())
          .feeAmount(bittrexOrder.getDelta().getCommission())
          .build();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a Balance object from a BittrexBalance message
   *
   * @param bittrexBalance
   * @return
   */
  public static Balance bittrexBalanceMessageToBalance(String bittrexBalanceBalance) {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      // decompress message
      String decompressedMessage = EncryptionUtils.decompress(bittrexBalanceBalance);
      LOG.debug("Decompressed balance message : {}", decompressedMessage);
      // parse JSON to Object
      BittrexBalance bittrexBalance =
          objectMapper.readValue(decompressedMessage, BittrexBalance.class);
      return new Balance.Builder()
          .currency(bittrexBalance.getDelta().getCurrencySymbol())
          .total(bittrexBalance.getDelta().getTotal())
          .available(bittrexBalance.getDelta().getAvailable())
          .timestamp(bittrexBalance.getDelta().getUpdatedAt())
          .build();
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
