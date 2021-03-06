package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexManualExample {
  private static final Logger LOG =
      LoggerFactory.getLogger(info.bitrich.xchangestream.bittrex.BittrexManualExample.class);

  public static void main(String[] args) {
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
//            .getStreamingAccountService()
//                    .getBalanceChanges(Currency.BTC)
//                    .subscribe(balance -> {
//                      LOG.info("Received balance : {}", balance);
//                    });
//        .getStreamingTradeService()
//        .getUserTrades(CurrencyPair.ETH_BTC)
//        .subscribe(
//            userTrade -> {
//              LOG.info("Received user trade {}", userTrade);
//            });
  }
}
