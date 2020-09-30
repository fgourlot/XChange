package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.core.*;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexTradeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingExchange extends BittrexExchange implements StreamingExchange {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingExchange.class);

  // Bittrex WebSocket API endpoint
  private static final String API_BASE_URI = "https://socket-v3.bittrex.com/signalr";

  /** xchange-stream-bittrex services */
  private BittrexStreamingService bittrexStreamingService;

  private BittrexStreamingAccountService bittrexStreamingAccountService;
  private BittrexStreamingMarketDataService bittrexStreamingMarketDataService;
  private BittrexStreamingTradeService bittrexStreamingTradeService;

  /** xchange-bittrex services */
  private BittrexAccountService bittrexAccountService;

  private BittrexMarketDataService bittrexMarketDataService;
  private BittrexTradeService bittrexTradeService;

  public BittrexStreamingExchange() {}

  protected void initServices() {
    super.initServices();
    ExchangeSpecification exchangeSpecification = this.getExchangeSpecification();
    bittrexAccountService = new BittrexAccountService(this);
    bittrexMarketDataService = new BittrexMarketDataService(this);
    bittrexTradeService = new BittrexTradeService(this);
    bittrexStreamingService = new BittrexStreamingService(API_BASE_URI, exchangeSpecification);
    bittrexStreamingAccountService =
        new BittrexStreamingAccountService(bittrexStreamingService, bittrexAccountService);
    bittrexStreamingMarketDataService =
        new BittrexStreamingMarketDataService(bittrexStreamingService, bittrexMarketDataService);
    bittrexStreamingTradeService =
        new BittrexStreamingTradeService(bittrexStreamingService, bittrexTradeService);
  }

  public io.reactivex.Completable connect(ProductSubscription... args) {
    return this.bittrexStreamingService.connect();
  }

  public io.reactivex.Completable disconnect() {
    this.bittrexStreamingService.disconnect();
    return null;
  }

  @Override
  public boolean isAlive() {
    return bittrexStreamingService.isAlive();
  }

  public StreamingMarketDataService getStreamingMarketDataService() {
    return bittrexStreamingMarketDataService;
  }

  public StreamingAccountService getStreamingAccountService() {
    return bittrexStreamingAccountService;
  }

  public StreamingTradeService getStreamingTradeService() {
    return bittrexStreamingTradeService;
  }

  public void useCompressedMessages(boolean compressedMessages) {}
}
