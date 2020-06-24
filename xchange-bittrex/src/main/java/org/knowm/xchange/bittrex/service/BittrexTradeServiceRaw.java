package org.knowm.xchange.bittrex.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.dto.trade.BittrexOrderV3;
import org.knowm.xchange.bittrex.dto.trade.BittrexUserTrade;
import org.knowm.xchange.bittrex.service.batch.BatchOrderResponse;
import org.knowm.xchange.bittrex.service.batch.order.BatchOrder;
import org.knowm.xchange.bittrex.service.batch.order.neworder.NewOrderPayload;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.MarketOrder;
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

  /**
   * @deprecated Endpoint still valid, but Bittrex have disabled market orders. See
   *     https://twitter.com/bittrexexchange/status/526590250487783425.
   */
  @Deprecated
  public String placeBittrexMarketOrder(MarketOrder marketOrder) throws IOException {

    return (OrderType.BID.equals(marketOrder.getType())
            ? bittrexAuthenticated.buymarket(
                apiKey,
                signatureCreator,
                exchange.getNonceFactory(),
                BittrexUtils.toPairString(marketOrder.getCurrencyPair()),
                marketOrder.getOriginalAmount().toPlainString())
            : bittrexAuthenticated.sellmarket(
                apiKey,
                signatureCreator,
                exchange.getNonceFactory(),
                BittrexUtils.toPairString(marketOrder.getCurrencyPair()),
                marketOrder.getOriginalAmount().toPlainString()))
        .getResult()
        .getUuid();
  }

  public String placeBittrexLimitOrder(LimitOrder limitOrder) throws IOException {

    return (OrderType.BID.equals(limitOrder.getType())
            ? bittrexAuthenticated.buylimit(
                apiKey,
                signatureCreator,
                exchange.getNonceFactory(),
                BittrexUtils.toPairString(limitOrder.getCurrencyPair()),
                limitOrder.getOriginalAmount().toPlainString(),
                limitOrder.getLimitPrice().toPlainString())
            : bittrexAuthenticated.selllimit(
                apiKey,
                signatureCreator,
                exchange.getNonceFactory(),
                BittrexUtils.toPairString(limitOrder.getCurrencyPair()),
                limitOrder.getOriginalAmount().toPlainString(),
                limitOrder.getLimitPrice().toPlainString()))
        .getResult()
        .getUuid();
  }

  public boolean cancelBittrexLimitOrder(String uuid) throws IOException {

    bittrexAuthenticated.cancel(apiKey, signatureCreator, exchange.getNonceFactory(), uuid);
    return true;
  }

  public List<BittrexOrderV3> getBittrexOpenOrders(OpenOrdersParams params) throws IOException {
    return bittrexAuthenticatedV3
        .getOpenOrders(apiKey,
                       System.currentTimeMillis(),
                       contentCreator,
                       signatureCreatorV3);
  }

  public List<BittrexUserTrade> getBittrexTradeHistory(CurrencyPair currencyPair)
      throws IOException {

    String ccyPair = currencyPair == null ? null : BittrexUtils.toPairString(currencyPair);
    return bittrexAuthenticated
        .getorderhistory(apiKey, signatureCreator, exchange.getNonceFactory(), ccyPair)
        .getResult();
  }

  public BittrexOrderV3 getBittrexOrder(String orderId) throws IOException {
    return bittrexAuthenticatedV3
        .getOrder(apiKey,
                  System.currentTimeMillis(),
                  contentCreator,
                  signatureCreatorV3, orderId);
  }

  public BatchOrderResponse[] executeOrdersBatch(BatchOrder[] batchOrders) throws IOException {
    return bittrexAuthenticatedV3.executeOrdersBatch(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, batchOrders);
  }

  public Map<String, Object> cancelOrderV3(String orderId) throws IOException {
    return bittrexAuthenticatedV3.cancelOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, orderId);
  }

  public Map<String, Object> placeOrderV3(NewOrderPayload newOrderPayload) throws IOException {
    return bittrexAuthenticatedV3.placeOrder(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3, newOrderPayload);
  }
}
