package info.bitrich.xchangesrteam.bittrex;

import com.github.signalr4j.client.ConnectionState;
import info.bitrich.xchangestream.bittrex.BittrexStreamingExchange;
import info.bitrich.xchangestream.bittrex.BittrexStreamingService;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;

import java.util.Timer;

public class BittrexStreamingServiceTest {

    ExchangeSpecification exchangeSpecification;
    StreamingExchange exchange;
    BittrexStreamingService streamingService;

    private static final String API_BASE_URI = "https://socket-v3.bittrex.com/signalr";

    @Before
    public void setup() {
        String apiKey = System.getProperty("apiKey");
        String apiSecret = System.getProperty("apiSecret");
        exchangeSpecification = new ExchangeSpecification(BittrexStreamingExchange.class.getName());
        exchangeSpecification.setApiKey(apiKey);
        exchangeSpecification.setSecretKey(apiSecret);
        exchange = StreamingExchangeFactory.INSTANCE.createExchange(exchangeSpecification);
        streamingService = new BittrexStreamingService(API_BASE_URI, exchangeSpecification);
    }

    @Test
    public void connectTest() {
        Assert.assertTrue(streamingService.getConnexionState() == ConnectionState.Disconnected);
        exchange.connect().blockingAwait();
        Assert.assertTrue(streamingService.getConnexionState() == ConnectionState.Connected);
    }
}
