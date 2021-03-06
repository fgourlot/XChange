package info.bitrich.xchangestream.bittrex;

import io.reactivex.disposables.Disposable;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class BittrexStreamingMarketDataServiceTest extends BittrexStreamingBaseTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BittrexStreamingMarketDataServiceTest.class);
  CurrencyPair market = CurrencyPair.ETH_BTC;
  static Optional<Timer> timer;

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
    ArrayList<OrderBook> booksWS = new ArrayList<>();
    ArrayList<OrderBook> booksRest = new ArrayList<>();

    // WS orderbook map filling
    AtomicBoolean canStartRestFilling = new AtomicBoolean();
    Disposable wsDisposable =
        exchange
            .getStreamingMarketDataService()
            .getOrderBook(market)
            .subscribe(
                orderBook -> {
                  LOG.debug("Received order book {}", orderBook);
                  booksWS.add(orderBook);
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
                    OrderBook orderBook = exchange.getMarketDataService().getOrderBook(market);
                    booksRest.add(orderBook);
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
      timer.ifPresent(Timer::cancel);
      wsDisposable.dispose();
    }

    // Test we have fetched orderbooks
    Assert.assertTrue(booksWS.size() > 0);
    Assert.assertTrue(booksRest.size() > 0);

    // Try to find the rest books in ws books list
    Collection<Integer> indexes =
        booksRest.stream().map(book -> findBookInList(book, booksWS)).collect(Collectors.toList());
    // Check that all the rest books were found in ws books
    Assert.assertTrue(indexes.stream().allMatch(index -> index > 0));
    // Check that the books are chronologically found
    Assert.assertEquals(indexes.stream().sorted().collect(Collectors.toList()), indexes);
  }

  private int findBookInList(OrderBook bookToFind, ArrayList<OrderBook> books) {
    return books.stream()
        .filter(bookToFind::ordersEqual)
        .findFirst()
        .map(books::indexOf)
        .orElse(-1);
  }

  @AfterClass
  public static void dispose() {
    timer.ifPresent(Timer::cancel);
  }
}
