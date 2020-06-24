package org.knowm.xchange.bittrex.service;

import java.io.IOException;
import java.util.List;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.BittrexAdapters;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepthV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexMarketSummaryV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexSymbolV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTickerV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTradeV3;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.trade.LimitOrder;

import lombok.Data;

public class BittrexMarketDataServiceRaw extends BittrexBaseService {

  /**
   * Constructor
   *
   * @param exchange
   */
  public BittrexMarketDataServiceRaw(Exchange exchange) {
    super(exchange);
  }

  public List<BittrexSymbolV3> getBittrexSymbols() throws IOException {
    return bittrexAuthenticatedV3.getMarkets();
  }

  public BittrexMarketSummaryV3 getBittrexMarketSummary(String pair) throws IOException {
    return bittrexAuthenticatedV3.getMarketSummary(pair);
  }

  public List<BittrexMarketSummaryV3> getBittrexMarketSummaries() throws IOException {
    return bittrexAuthenticatedV3.getMarketSummaries();
  }

  public BittrexTickerV3 getBittrexTicker(String pair) throws IOException {
    return bittrexAuthenticatedV3.getTicker(pair);
  }

  public List<BittrexTickerV3> getBittrexTickers() throws IOException {
    return bittrexAuthenticatedV3.getTickers();
  }

  public SequencedOrderBook getBittrexSequencedOrderBook(String market, int depth)
      throws IOException {
    BittrexDepthV3 bittrexDepthV3 = bittrexAuthenticatedV3.getBookV3(market, depth);

    CurrencyPair currencyPair = BittrexUtils.toCurrencyPair(market);
    List<LimitOrder> asks =
        BittrexAdapters.adaptOrdersV3(bittrexDepthV3.getAsks(), currencyPair, "ask", null, depth);
    List<LimitOrder> bids =
        BittrexAdapters.adaptOrdersV3(bittrexDepthV3.getBids(), currencyPair, "bid", null, depth);

    OrderBook orderBook = new OrderBook(null, asks, bids);
    return new SequencedOrderBook(bittrexDepthV3.getSequence(), orderBook);
  }

  public List<BittrexTradeV3> getBittrexTrades(String pair) throws IOException {
    return bittrexAuthenticatedV3.getTrades(pair);
  }

  @Data
  public static class SequencedOrderBook {
    private final String sequence;
    private final OrderBook orderBook;
  }
}
