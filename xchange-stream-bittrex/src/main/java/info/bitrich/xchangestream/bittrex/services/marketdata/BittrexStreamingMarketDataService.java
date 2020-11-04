package info.bitrich.xchangestream.bittrex.services.marketdata;

import info.bitrich.xchangestream.bittrex.services.BittrexStreamingService;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import io.reactivex.Observable;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trade;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;

/** See https://bittrex.github.io/api/v3#topic-Websocket-Overview */
public class BittrexStreamingMarketDataService implements StreamingMarketDataService {

  private final BittrexStreamingOrderBookService bittrexStreamingOrderBookService;

  public BittrexStreamingMarketDataService(
      BittrexStreamingService bittrexStreamingService, BittrexMarketDataService marketDataService) {
    this.bittrexStreamingOrderBookService =
        new BittrexStreamingOrderBookService(bittrexStreamingService, marketDataService);
  }

  @Override
  public Observable<OrderBook> getOrderBook(CurrencyPair currencyPair, Object... args) {
    return bittrexStreamingOrderBookService.getOrderBook(currencyPair);
  }

  @Override
  public Observable<Ticker> getTicker(CurrencyPair currencyPair, Object... args) {
    throw new NotYetImplementedForExchangeException();
  }

  @Override
  public Observable<Trade> getTrades(CurrencyPair currencyPair, Object... args) {
    throw new NotYetImplementedForExchangeException();
  }
}
