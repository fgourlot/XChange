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
    return bittrexAuthenticatedV3
        .placeOrder(
            apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, bittrexNewOrder)
        .getId();
  }

  public BittrexOrderV3 cancelBittrexLimitOrder(String orderId) throws IOException {
    return bittrexAuthenticatedV3.cancelOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, orderId);
  }

  public List<BittrexOrderV3> getBittrexOpenOrders(OpenOrdersParams params) throws IOException {
    return bittrexAuthenticatedV3.getOpenOrders(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3);
  }

  public List<BittrexOrderV3> getBittrexTradeHistory(CurrencyPair currencyPair) throws IOException {
    return bittrexAuthenticatedV3.getClosedOrders(
        apiKey,
        System.currentTimeMillis(),
        contentCreator,
        signatureCreatorV3,
        BittrexUtils.toPairString(currencyPair),
        200);
  }

  public List<BittrexOrderV3> getBittrexTradeHistory() throws IOException {
    return getBittrexTradeHistory(null);
  }

  public BittrexOrderV3 getBittrexOrder(String orderId) throws IOException {
    return bittrexAuthenticatedV3.getOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, orderId);
  }

  public BatchOrderResponse[] executeOrdersBatch(BatchOrder[] batchOrders) throws IOException {
    return bittrexAuthenticatedV3.executeOrdersBatch(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, batchOrders);
  }

  public BittrexOrderV3 cancelOrderV3(String orderId) throws IOException {
    return bittrexAuthenticatedV3.cancelOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, orderId);
  }

  public BittrexOrderV3 placeOrderV3(BittrexNewOrder bittrexNewOrder) throws IOException {
    return bittrexAuthenticatedV3.placeOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, bittrexNewOrder);
  }
}
