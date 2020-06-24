package org.knowm.xchange.bittrex.service;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.BittrexAdapters;
import org.knowm.xchange.bittrex.BittrexErrorAdapter;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.dto.BittrexException;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexChartData;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexCurrency;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepth;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepthV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexMarketSummary;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexSymbol;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTicker;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTrade;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexV2MarketSummary;
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

  public List<BittrexCurrency> getBittrexCurrencies() throws IOException {

    return bittrexAuthenticated.getCurrencies().getResult();
  }

  public BittrexTicker getBittrexTicker(CurrencyPair currencyPair) throws IOException {
    return bittrexAuthenticated.getTicker(BittrexUtils.toPairString(currencyPair)).getResult();
  }

  public List<BittrexSymbol> getBittrexSymbols() throws IOException {

    return bittrexAuthenticated.getSymbols().getResult();
  }

  public BittrexMarketSummary getBittrexMarketSummary(String pair) throws IOException {

    List<BittrexMarketSummary> result = bittrexAuthenticated.getMarketSummary(pair).getResult();
    if (result == null || result.isEmpty()) {
      return null;
    }
    return result.get(0);
  }

  public List<BittrexMarketSummary> getBittrexMarketSummaries() throws IOException {

    return bittrexAuthenticated.getMarketSummaries().getResult();
  }

  public BittrexDepth getBittrexOrderBook(String pair, int depth) throws IOException {

    return bittrexAuthenticated.getBook(pair, "both", depth).getResult();
  }

  public SequencedOrderBook getBittrexSequencedOrderBook(String market, int depth)
      throws IOException {
    BittrexDepthV3 bittrexDepthV3 = bittrexAuthenticatedV3.getBookV3(market, depth);

    CurrencyPair currencyPair = BittrexUtils.toCurrencyPair(market, true);
    List<LimitOrder> asks =
        BittrexAdapters.adaptOrdersV3(bittrexDepthV3.getAsks(), currencyPair, "ask", null, depth);
    List<LimitOrder> bids =
        BittrexAdapters.adaptOrdersV3(bittrexDepthV3.getBids(), currencyPair, "bid", null, depth);

    OrderBook orderBook = new OrderBook(null, asks, bids);
    return new SequencedOrderBook(bittrexDepthV3.getSequence(), orderBook);
  }

  public List<BittrexTrade> getBittrexTrades(String pair) throws IOException {

    return bittrexAuthenticated.getTrades(pair).getResult();
  }

  public List<BittrexChartData> getBittrexChartData(
      CurrencyPair currencyPair, BittrexChartDataPeriodType periodType) throws IOException {

    return bittrexV2
        .getChartData(BittrexUtils.toPairString(currencyPair), periodType.getPeriod())
        .getResult();
  }

  public List<BittrexChartData> getBittrexLatestTick(
      CurrencyPair currencyPair, BittrexChartDataPeriodType periodType, Long timeStamp)
      throws IOException {
    return bittrexV2
        .getLatestTick(BittrexUtils.toPairString(currencyPair), periodType.getPeriod(), timeStamp)
        .getResult();
  }

  public List<BittrexV2MarketSummary> getBittrexV2MarketSummaries() {
    return bittrexV2.getMarketSummaries().getResult();
  }

  @Data
  public static class SequencedOrderBook {
    private final String sequence;
    private final OrderBook orderBook;
  }
}
