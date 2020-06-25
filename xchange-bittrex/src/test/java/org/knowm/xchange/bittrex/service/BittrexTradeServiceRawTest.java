package org.knowm.xchange.bittrex.service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.bittrex.dto.account.BittrexBalanceV3;
import org.knowm.xchange.bittrex.dto.trade.BittrexOrder;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.knowm.xchange.dto.trade.LimitOrder;

import junit.framework.TestCase;

public class BittrexTradeServiceRawTest extends TestCase {
  @Test
  public void test() throws IOException {
    ExchangeSpecification exSpec = new BittrexExchange().getDefaultExchangeSpecification();
    String apiKey = System.getProperty("apiKey");
    String apiSecret = System.getProperty("apiSecret");
    exSpec.setApiKey(apiKey);
    exSpec.setSecretKey(apiSecret);
    Exchange bittrex = ExchangeFactory.INSTANCE.createExchange(exSpec);
    BittrexAccountService accountService = (BittrexAccountService) bittrex.getAccountService();
    BittrexTradeService tradeService = (BittrexTradeService) bittrex.getTradeService();
    BittrexMarketDataService marketDataService =
        (BittrexMarketDataService) bittrex.getMarketDataService();

    // Account service tests
    Collection<BittrexBalanceV3> bittrexBalancesV3 = accountService.getBittrexBalances();
    System.out.println(bittrexBalancesV3.toString());
    Map<Currency, Balance> bittrexBalances =
        accountService.getAccountInfo().getWallet().getBalances();

    // Trade service tests
    //List<BittrexOrderV3> bittrexOpenOrders = ((BittrexTradeServiceRaw) tradeService).getBittrexOpenOrders(null);


    /*LimitOrder order = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.ETH_BTC)
        .originalAmount(new BigDecimal("0.1"))
        .limitPrice(new BigDecimal("0.01")).build();
    String s = tradeService.placeLimitOrder(order);*/

    List<BittrexOrder> bittrexTradeHistory = ((BittrexTradeServiceRaw) tradeService).getBittrexTradeHistory(null);

    // Market data service tests
    List<Ticker> tickers = marketDataService.getTickers(null);

    System.out.println();
  }
}
