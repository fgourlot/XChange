package info.bitrich.xchangestream.bittrex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexManualExample {
  private static final Logger LOG =
      LoggerFactory.getLogger(info.bitrich.xchangestream.bittrex.BittrexManualExample.class);

  public static void main(String[] args) throws InterruptedException, IOException {
    ExchangeSpecification exchangeSpecification =
        new ExchangeSpecification(BittrexStreamingExchange.class.getName());
    exchangeSpecification.setApiKey(args[0]);
    exchangeSpecification.setSecretKey(args[1]);

    StreamingExchange streamingExchange =
        StreamingExchangeFactory.INSTANCE.createExchange(exchangeSpecification);
    streamingExchange.connect().blockingAwait();

    List<Disposable> disposables = new ArrayList<>();
    ConcurrentMap<CurrencyPair, OrderBook> books = new ConcurrentHashMap<>();
    ConcurrentMap<CurrencyPair, AtomicInteger> updatesCount = new ConcurrentHashMap<>();
    Exchange exchange =
        ExchangeFactory.INSTANCE.createExchange(BittrexExchange.class.getName());
    exchange.getMarketDataService().getTickers(null).stream()
        .map(Ticker::getCurrencyPair)
        .forEach(market -> {
          try {
            // we don't want http 429
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          Disposable disposable =
              streamingExchange
                  .getStreamingMarketDataService()
                  .getOrderBook(market)
                  .subscribe(
                      orderBook -> {
                        LOG.info("Received order book {}", market);
                        books.put(market, orderBook);
                        updatesCount.putIfAbsent(market, new AtomicInteger(0));
                        updatesCount.get(market).incrementAndGet();
                      });
          disposables.add(disposable);
        });
    Thread.sleep(600_000);
    disposables.forEach(Disposable::dispose);
  }
}
