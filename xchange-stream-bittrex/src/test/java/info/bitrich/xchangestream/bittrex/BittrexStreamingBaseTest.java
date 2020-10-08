package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import org.junit.After;
import org.junit.Before;
import org.knowm.xchange.ExchangeSpecification;

public class BittrexStreamingBaseTest {

  ExchangeSpecification exchangeSpecification;
  StreamingExchange exchange;
  ObjectMapper objectMapper;

  @Before
  public void setup() {
    String apiKey = System.getProperty("apiKey");
    String apiSecret = System.getProperty("apiSecret");
    exchangeSpecification = new ExchangeSpecification(BittrexStreamingExchange.class.getName());
    exchangeSpecification.setApiKey(apiKey);
    exchangeSpecification.setSecretKey(apiSecret);
    exchange = StreamingExchangeFactory.INSTANCE.createExchange(exchangeSpecification);
    exchange.connect().blockingAwait();
    objectMapper = new ObjectMapper();
  }

  @After
  public void tearDown() {
    exchange.disconnect();
  }
}
