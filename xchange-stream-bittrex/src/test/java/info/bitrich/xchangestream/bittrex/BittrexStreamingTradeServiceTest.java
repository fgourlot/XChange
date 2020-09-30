package info.bitrich.xchangestream.bittrex;

import io.reactivex.disposables.Disposable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingTradeServiceTest extends BittrexStreamingBaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingTradeServiceTest.class);

  @Test
  public void testOrders() throws InterruptedException, IOException {
    Map<CurrencyPair, List<Order>> ordersChanges = new HashMap<>();
    List<Disposable> disposables = new ArrayList<>();
    final Object streamLock = new Object();
    exchange.getMarketDataService().getTickers(null).stream()
        .map(Ticker::getCurrencyPair)
        .forEach(
            market -> {
              Disposable wsDisposable =
                  exchange
                      .getStreamingTradeService()
                      .getOrderChanges(market)
                      .subscribe(
                          order -> {
                            synchronized (streamLock) {
                              LOG.debug("Received order update {}", order);
                              ordersChanges.putIfAbsent(market, new ArrayList<>());
                              ordersChanges.get(market).add(order);
                            }
                          });
              disposables.add(wsDisposable);
            });
    Thread.sleep(300_000);
    disposables.forEach(Disposable::dispose);
  }
}
