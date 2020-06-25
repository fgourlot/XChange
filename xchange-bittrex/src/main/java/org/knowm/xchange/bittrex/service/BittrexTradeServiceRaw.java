package org.knowm.xchange.bittrex.service;

import java.io.IOException;
import java.util.List;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.dto.trade.BittrexNewOrder;
import org.knowm.xchange.bittrex.dto.trade.BittrexOrderV3;
import org.knowm.xchange.bittrex.service.batch.BatchOrderResponse;
import org.knowm.xchange.bittrex.service.batch.order.BatchOrder;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.service.trade.params.orders.OpenOrdersParams;

public class BittrexTradeServiceRaw extends BittrexBaseService {

  /**
   * Constructor
   *
   * @param exchange
   */
  public BittrexTradeServiceRaw(Exchange exchange) {
    super(exchange);
  }

  public String placeBittrexLimitOrder(LimitOrder limitOrder) throws IOException {
    BittrexNewOrder bittrexNewOrder =
        new BittrexNewOrder(
            BittrexUtils.toPairString(limitOrder.getCurrencyPair()),
            OrderType.BID.equals(limitOrder.getType()) ? "BUY" : "SELL",
            "LIMIT",
            limitOrder.getRemainingAmount(),
            null,
            limitOrder.getLimitPrice(),
            "GOOD_TIL_CANCELLED",
            null,
            null);
    return bittrexAuthenticated
        .placeOrder(
            apiKey, System.currentTimeMillis(), contentCreator, signatureCreator, bittrexNewOrder)
        .getId();
  }

  public BittrexOrderV3 cancelBittrexLimitOrder(String orderId) throws IOException {
    return bittrexAuthenticated.cancelOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreator, orderId);
  }

  public List<BittrexOrderV3> getBittrexOpenOrders(OpenOrdersParams params) throws IOException {
    return bittrexAuthenticated.getOpenOrders(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreator);
  }

  public List<BittrexOrderV3> getBittrexTradeHistory(CurrencyPair currencyPair) throws IOException {
    return bittrexAuthenticated.getClosedOrders(
        apiKey,
        System.currentTimeMillis(),
        contentCreator,
        signatureCreator,
        BittrexUtils.toPairString(currencyPair),
        200);
  }

  public List<BittrexOrderV3> getBittrexTradeHistory() throws IOException {
    return getBittrexTradeHistory(null);
  }

  public BittrexOrderV3 getBittrexOrder(String orderId) throws IOException {
    return bittrexAuthenticated.getOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreator, orderId);
  }

  public BatchOrderResponse[] executeOrdersBatch(BatchOrder[] batchOrders) throws IOException {
    return bittrexAuthenticated.executeOrdersBatch(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreator, batchOrders);
  }

  public BittrexOrderV3 cancelOrderV3(String orderId) throws IOException {
    return bittrexAuthenticated.cancelOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreator, orderId);
  }

  public BittrexOrderV3 placeOrderV3(BittrexNewOrder bittrexNewOrder) throws IOException {
    return bittrexAuthenticated.placeOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreator, bittrexNewOrder);
  }
}
