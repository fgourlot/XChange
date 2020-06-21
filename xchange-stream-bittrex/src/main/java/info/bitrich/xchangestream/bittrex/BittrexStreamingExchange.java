package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingAccountService;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingExchange extends BittrexExchange implements StreamingExchange {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingExchange.class);

  private static final String API_BASE_URI = "https://socket-v3.bittrex.com/signalr";

  private BittrexStreamingService streamingService;
  private BittrexStreamingAccountService streamingAccountService;
  private BittrexStreamingMarketDataService streamingMarketDataService;
  private BittrexMarketDataService bittrexMarketDataService;

  public BittrexStreamingExchange() {}

  protected void initServices() {
    super.initServices();
    ExchangeSpecification exchangeSpecification = this.getExchangeSpecification();
    streamingService = new BittrexStreamingService(API_BASE_URI, exchangeSpecification);
    streamingAccountService = new BittrexStreamingAccountService(streamingService);
    bittrexMarketDataService = new BittrexMarketDataService(this);
    streamingMarketDataService =
        new BittrexStreamingMarketDataService(streamingService, bittrexMarketDataService);
  }

  public io.reactivex.Completable connect(ProductSubscription... args) {
    return this.streamingService.connect();
  }

  public io.reactivex.Completable disconnect() {
    this.streamingService.disconnect();
    return null;
  }

  public boolean isAlive() {
    return false;
  }

  public StreamingMarketDataService getStreamingMarketDataService() {
    return streamingMarketDataService;
  }

  public StreamingAccountService getStreamingAccountService() {
    return streamingAccountService;
  }

  public void useCompressedMessages(boolean compressedMessages) {}
}
