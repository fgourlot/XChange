package info.bitrich.xchangestream.bittrex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;

import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexManualExample {
  private static final Logger LOG =
      LoggerFactory.getLogger(info.bitrich.xchangestream.bittrex.BittrexManualExample.class);

  public static void main(String[] args) throws InterruptedException {
    ExchangeSpecification exchangeSpecification =
        new ExchangeSpecification(BittrexStreamingExchange.class.getName());
    exchangeSpecification.setApiKey(args[0]);
    exchangeSpecification.setSecretKey(args[1]);
    StreamingExchange exchange =
        StreamingExchangeFactory.INSTANCE.createExchange(exchangeSpecification);
    exchange.connect().blockingAwait();

    ConcurrentMap<CurrencyPair, List<OrderBook>> books = new ConcurrentHashMap<>();

    Disposable subscribe =
        exchange
            .getStreamingMarketDataService()
            .getOrderBook(CurrencyPair.BTC_USD)
            .subscribe(
                orderBook -> {
                  LOG.info("Received order book {}", CurrencyPair.BTC_USD);
                  books.putIfAbsent(orderBook.getAsks().get(0).getCurrencyPair(), new ArrayList<>());
                  books.get(orderBook.getAsks().get(0).getCurrencyPair()).add(orderBook);
                });
    Disposable subscribe2 =
        exchange
            .getStreamingMarketDataService()
            .getOrderBook(CurrencyPair.LTC_BTC)
            .subscribe(
                orderBook -> {
                  LOG.info("Received order book {}", CurrencyPair.LTC_BTC);
                  books.putIfAbsent(orderBook.getAsks().get(0).getCurrencyPair(), new ArrayList<>());
                  books.get(orderBook.getAsks().get(0).getCurrencyPair()).add(orderBook);
                });

    Thread.sleep(10_000);
    subscribe.dispose();
    subscribe2.dispose();
  }
}
