package info.bitrich.xchangesrteam.bittrex;

import info.bitrich.xchangestream.bittrex.BittrexStreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;

import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.service.account.AccountService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BittrexManualExample {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexManualExample.class);



  public static void main(String[] args) throws IOException {
    ExchangeSpecification exchangeSpecification =
        new ExchangeSpecification(BittrexStreamingExchange.class.getName());
    exchangeSpecification.setApiKey(args[0]);
    exchangeSpecification.setSecretKey(args[1]);
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

  ExchangeSpecification exchangeSpecification;
  String apiKey = "";
  String apiSecret = "";
  StreamingExchange exchange;

  @Before
  public void init(){
    exchangeSpecification = new ExchangeSpecification(BittrexStreamingExchange.class.getName());
    exchangeSpecification.setApiKey("78cf1e6997c4443b88729a5de3b60174");
    exchangeSpecification.setSecretKey("8930cfe72ae7429c8d8fa5bb69b2958c");
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
}
