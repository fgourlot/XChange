package org.knowm.xchange.bittrex.service;

import java.io.IOException;
import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.BittrexAdapters;
import org.knowm.xchange.bittrex.BittrexErrorAdapter;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.bittrex.dto.BittrexException;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexMarketSummary;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexMarketSummaryV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTickerV3;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTrade;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexTradeV3;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.service.marketdata.MarketDataService;
import org.knowm.xchange.service.marketdata.params.CurrencyPairsParam;
import org.knowm.xchange.service.marketdata.params.Params;

/**
 * Implementation of the market data service for Bittrex
 *
 * <ul>
 *   <li>Provides access to various market data values
 * </ul>
 */
public class BittrexMarketDataService extends BittrexMarketDataServiceRaw
    implements MarketDataService {

  /**
   * Constructor
   *
   * @param exchange
   */
  public BittrexMarketDataService(Exchange exchange) {

    super(exchange);
  }

  @Override
  public Ticker getTicker(CurrencyPair currencyPair, Object... args) throws IOException {
    String marketSymbol = BittrexUtils.toPairString(currencyPair);
    // The only way is to make two API calls since the information is split between market summary
    // and ticker calls...
    BittrexMarketSummaryV3 bittrexMarketSummaryV3 =
        bittrexAuthenticatedV3.getMarketSummary(marketSymbol);
    BittrexTickerV3 bittrexTickerV3 = bittrexAuthenticatedV3.getTicker(marketSymbol);
    return BittrexAdapters.adaptTicker(bittrexMarketSummaryV3, bittrexTickerV3);
  }

  @Override
  public List<Ticker> getTickers(Params params) throws IOException {
    try {
      List<CurrencyPair> currencyPairs =
          (params instanceof CurrencyPairsParam)
              ? new ArrayList<>(((CurrencyPairsParam) params).getCurrencyPairs())
              : new ArrayList<>();

      // The only way is to make two API calls since the information is split between market summary
      // and ticker calls...
      List<BittrexMarketSummaryV3> bittrexMarketSummaries = getBittrexMarketSummaries();
      List<BittrexTickerV3> bittrexTickers = getBittrexTickers();
      Map<CurrencyPair, Pair<BittrexMarketSummaryV3, BittrexTickerV3>> tickerCombinationMap =
          new HashMap<>(bittrexMarketSummaries.size());
      bittrexMarketSummaries.forEach(
          marketSummary ->
              tickerCombinationMap.put(
                  BittrexUtils.toCurrencyPair(marketSummary.getSymbol()),
                  Pair.of(marketSummary, null)));
      bittrexTickers.forEach(
          ticker -> {
            CurrencyPair currencyPair = BittrexUtils.toCurrencyPair(ticker.getSymbol());
            tickerCombinationMap.putIfAbsent(currencyPair, Pair.of(null, ticker));
            tickerCombinationMap.get(currencyPair).setValue(ticker);
          });

      return tickerCombinationMap.entrySet().stream()
          .filter(entry -> currencyPairs.contains(entry.getKey()))
          .map(
              entry ->
                  BittrexAdapters.adaptTicker(
                      entry.getValue().getKey(), entry.getValue().getValue()))
          .collect(Collectors.toList());
    } catch (BittrexException e) {
      throw BittrexErrorAdapter.adapt(e);
    }
  }

  /**
   * @param currencyPair
   * @param args
   * @return A pair of orderbook and its sequence.
   * @throws IOException
   */
  @Override
  public OrderBook getOrderBook(CurrencyPair currencyPair, Object... args) throws IOException {
    int depth = 500;

    if (args != null && args.length > 0) {
      if (args[0] instanceof Integer && (Integer) args[0] > 0 && (Integer) args[0] <= 500) {
        depth = (Integer) args[0];
      }
    }
    return getBittrexSequencedOrderBook(BittrexUtils.toPairString(currencyPair), depth)
        .getOrderBook();
  }

  /**
   * @param currencyPair The CurrencyPair for which to query trades.
   * @param args no further args are supported by the API
   */
  @Override
  public Trades getTrades(CurrencyPair currencyPair, Object... args) throws IOException {
    try {
      List<BittrexTradeV3> trades = getBittrexTrades(BittrexUtils.toPairString(currencyPair));

      return BittrexAdapters.adaptTrades(trades, currencyPair);
    } catch (BittrexException e) {
      throw BittrexErrorAdapter.adapt(e);
    }
  }
}
