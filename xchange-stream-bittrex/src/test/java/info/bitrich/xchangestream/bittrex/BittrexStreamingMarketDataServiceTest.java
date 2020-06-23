package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import org.junit.BeforeClass;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Timer;

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
}
