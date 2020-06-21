package info.bitrich.xchangestream.bittrex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepthV3;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;

import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookDeltas;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrderBookEntry;

public final class BittrexStreamingUtils {

  private BittrexStreamingUtils() {
  }

  public static Stream<LimitOrder> cloneOrders(Collection<LimitOrder> orders) {
    return orders.stream().map(order -> LimitOrder.Builder.from(order).build());
  }

  /**
   * Update a given OrderBook with Bittrex deltas in a BittrexOrderBook message
   *
   * @param orderBookToUpdate
   * @param updates
   * @return
   */
  public static OrderBook updateOrderBook(OrderBook orderBookToUpdate, BittrexOrderBookDeltas updates) {
    CurrencyPair market = BittrexUtils.toCurrencyPair(updates.getMarketSymbol(), true);
    applyUpdates(orderBookToUpdate, updates.getAskDeltas(), Order.OrderType.ASK, market);
    applyUpdates(orderBookToUpdate, updates.getBidDeltas(), Order.OrderType.BID, market);
    Map<String, Object> metadata = Map.of(BittrexDepthV3.SEQUENCE, updates.getSequence());
    orderBookToUpdate.setMetadata(metadata);
    return orderBookToUpdate;
  }

  /**
   * Effective orders updates method (add and delete)
   *
   * @param orderBookToUpdate
   * @param updates
   * @param orderType
   * @param market
   */
  public static void applyUpdates(OrderBook orderBookToUpdate,
                                  BittrexOrderBookEntry[] updates,
                                  Order.OrderType orderType,
                                  CurrencyPair market) {
    Arrays.stream(updates)
          .map(update -> new LimitOrder.Builder(orderType, market).originalAmount(update.getQuantity())
                                                                  .limitPrice(update.getRate())
                                                                  .build())
          .forEach(orderBookToUpdate::update);
  }

}
