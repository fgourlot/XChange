package info.bitrich.xchangesrteam.bittrex;

import info.bitrich.xchangestream.bittrex.BittrexStreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BittrexManualExample {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexManualExample.class);

  private static final String API_KEY = "";
  private static final String API_SECRET = "";

  public static void main(String[] args) throws IOException {
    ExchangeSpecification exchangeSpecification =
        new ExchangeSpecification(BittrexStreamingExchange.class.getName());
    exchangeSpecification.setApiKey(API_KEY);
    exchangeSpecification.setSecretKey(API_SECRET);
    StreamingExchange exchange =
        StreamingExchangeFactory.INSTANCE.createExchange(exchangeSpecification);
    exchange.connect().blockingAwait();
    exchange
        .getStreamingMarketDataService()
        .getOrderBook(CurrencyPair.ETH_BTC)
        .subscribe(
            orderBook -> {
              LOG.info("Received order book {}", orderBook);
            });
  }
}
