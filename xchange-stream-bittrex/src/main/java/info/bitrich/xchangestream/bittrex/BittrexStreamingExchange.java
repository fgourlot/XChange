package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingExchange extends BittrexExchange implements StreamingExchange {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingExchange.class);

  private static final String API_BASE_URI = "https://socket-v3.bittrex.com/signalr";

  private BittrexStreamingService streamingService;
  private BittrexStreamingMarketDataService streamingMarketDataService;

  public BittrexStreamingExchange() {}

  @Override
  protected void initServices() {
    super.initServices();
    ExchangeSpecification exchangeSpecification = this.getExchangeSpecification();
    streamingService = new BittrexStreamingService(API_BASE_URI, exchangeSpecification);
    streamingMarketDataService = new BittrexStreamingMarketDataService(streamingService);
  }

  @Override
  public io.reactivex.Completable connect(ProductSubscription... args) {
    return this.streamingService.connect();
  }

  @Override
  public io.reactivex.Completable disconnect() {
    this.streamingService.disconnect();
    return null;
  }

  @Override
  public boolean isAlive() {
    return false;
  }

  @Override
  public StreamingMarketDataService getStreamingMarketDataService() {
    return streamingMarketDataService;
  }

  @Override
  public void useCompressedMessages(boolean compressedMessages) {}
}
