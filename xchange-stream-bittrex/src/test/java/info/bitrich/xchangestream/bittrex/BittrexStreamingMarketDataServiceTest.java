package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import io.reactivex.disposables.Disposable;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.dto.marketdata.BittrexDepthV3;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BittrexStreamingMarketDataServiceTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingMarketDataServiceTest.class);
  static ExchangeSpecification exchangeSpecification;
  static CurrencyPair market = CurrencyPair.ETH_BTC;
  static StreamingExchange exchange;
  static Optional<Timer> timer;

  @BeforeClass
  public static void setup() {
    String apiKey = System.getProperty("apiKey");
    String apiSecret = System.getProperty("apiSecret");
    market = CurrencyPair.ETH_BTC;
    exchangeSpecification = new ExchangeSpecification(BittrexStreamingExchange.class.getName());
    exchangeSpecification.setApiKey(apiKey);
    exchangeSpecification.setSecretKey(apiSecret);
    exchange = StreamingExchangeFactory.INSTANCE.createExchange(exchangeSpecification);
    exchange.connect().blockingAwait();
  }

  @Test
  public void orderBookSubTest() {
    exchange
        .getStreamingMarketDataService()
        .getOrderBook(CurrencyPair.ETH_BTC)
        .subscribe(
            orderBook -> {
              LOG.info("Received order book {}", orderBook);
            });

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Compares what is retrieved via websocket with periodic rest gets - first subscribes via WS and
   * fill a map with the WS books as values, and book sequence as the key - then launches a timer
   * and fill a map with the rest books as values, and book sequence as the key - then checks that
   * the books from the rest map are in the WS map.
   */
  @Test
  public void orderBookSynchroTest() {
    // Maps to compare
    Map<String, OrderBook> bookMapWS = new ConcurrentHashMap<>();
    Map<String, OrderBook> bookMapRest = new ConcurrentHashMap<>();

    // WS orderbook map filling
    AtomicBoolean canStartRestFilling = new AtomicBoolean();
    Disposable wsDisposable =
        exchange
            .getStreamingMarketDataService()
            .getOrderBook(market)
            .subscribe(
                orderBook -> {
                  LOG.debug("Received order book {}", orderBook);
                  bookMapWS.put(
                      orderBook.getMetadata().get(BittrexDepthV3.SEQUENCE).toString(), orderBook);
                  canStartRestFilling.set(true);
                });

    // Timed Rest orderbook map filling, 5s period
    timer = Optional.of(new Timer());
    timer
        .get()
        .scheduleAtFixedRate(
            new TimerTask() {
              public void run() {
                if (canStartRestFilling.get()) {
                  try {
                    Pair<OrderBook, String> orderBookV3 =
                        ((BittrexMarketDataService) exchange.getMarketDataService())
                            .getOrderBookV3(market);
                    bookMapRest.put(orderBookV3.getRight(), orderBookV3.getLeft());
                  } catch (IOException e) {
                    LOG.error("Error rest-getting the orderbook", e);
                  }
                }
              }
            },
            0,
            TimeUnit.SECONDS.toMillis(3));

    // Let it run for 20_000ms
    try {
      Thread.sleep(30_000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      // Stopping everyone
      wsDisposable.dispose();
      timer.ifPresent(Timer::cancel);
    }

    // Test we have fetched orderbooks
    Assert.assertTrue(bookMapWS.size() > 0);
    Assert.assertTrue(bookMapRest.size() > 0);

    // Check the books are equal
    bookMapRest.entrySet().stream()
        // We discard the REST books outside of the WS running period, in case the REST filling
        // started or ended outside of the WS
        // connection window
        .filter(
            restBookMapEntry ->
                bookMapWS.keySet().stream()
                    .anyMatch(
                        wsSeq -> {
                          long restSeqLong = Long.parseLong(restBookMapEntry.getKey());
                          long wsSeqLong = Long.parseLong(wsSeq);
                          return restSeqLong <= wsSeqLong && wsSeqLong <= restSeqLong;
                        }))
        .forEach(
            restBookMapEntry -> {
              OrderBook orderBookWS = bookMapWS.get(restBookMapEntry.getKey());
              Assert.assertNotNull(orderBookWS);
              // using OrderBook.ordersEqual to prevent from comparing the timestamps
              Assert.assertTrue(orderBookWS.ordersEqual(restBookMapEntry.getValue()));
            });
  }

  @AfterClass
  public static void dispose() {
    timer.ifPresent(Timer::cancel);
  }
}
