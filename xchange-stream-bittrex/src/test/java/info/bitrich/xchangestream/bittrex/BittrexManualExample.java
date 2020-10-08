package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.BittrexExchange;
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
    Exchange exchange = ExchangeFactory.INSTANCE.createExchange(BittrexExchange.class.getName());

    AtomicReference<LocalDateTime> lastUpdate = new AtomicReference<>(LocalDateTime.now());
    AtomicReference<Duration> timewithnoupdate = new AtomicReference<>(Duration.ZERO);
    AtomicInteger booksUpdatesCount = new AtomicInteger(0);
    AtomicReference<CurrencyPair> lastUpdatedMarket = new AtomicReference<>(CurrencyPair.ETH_BTC);

    new Timer()
        .scheduleAtFixedRate(
            new TimerTask() {
              public void run() {
                LOG.info("last update time: " + lastUpdate);
                LOG.info("Max duration: " + timewithnoupdate);
                LOG.info("Books updates count: " + booksUpdatesCount);
                LOG.info("Last updated market: " + lastUpdatedMarket);
              }
            },
            0,
            3_000);

    exchange.getMarketDataService().getTickers(null).stream()
        .map(Ticker::getCurrencyPair)
        // Stream.of(CurrencyPair.BTC_USD)
        .forEach(
            market -> {
              try {
                // we don't want http 429, getOrderBook limit is 600/mn
                Thread.sleep(100);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              Disposable disposable =
                  streamingExchange
                      .getStreamingMarketDataService()
                      .getOrderBook(market)
                      .subscribe(
                          orderBook -> {
                            booksUpdatesCount.incrementAndGet();
                            if (orderBook.getAsks() != null && !orderBook.getAsks().isEmpty()) {
                              lastUpdatedMarket.set(orderBook.getAsks().get(0).getCurrencyPair());
                            }
                            if (Duration.between(lastUpdate.get(), LocalDateTime.now())
                                    .compareTo(timewithnoupdate.get())
                                > 0) {
                              timewithnoupdate.set(
                                  Duration.between(lastUpdate.get(), LocalDateTime.now()));
                            }
                            lastUpdate.set(LocalDateTime.now());
                            // LOG.info("Received order book {}", market);
                            books.put(market, orderBook);
                            updatesCount.putIfAbsent(market, new AtomicInteger(0));
                            updatesCount.get(market).incrementAndGet();
                          });
              disposables.add(disposable);
            });
    Thread.sleep(7200_000);
    disposables.forEach(Disposable::dispose);
    LOG.info("subscribers disposed!");
  }
}
