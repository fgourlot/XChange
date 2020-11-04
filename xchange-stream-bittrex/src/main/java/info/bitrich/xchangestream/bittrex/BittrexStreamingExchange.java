package info.bitrich.xchangestream.bittrex;

import info.bitrich.xchangestream.bittrex.services.BittrexStreamingService;
import info.bitrich.xchangestream.bittrex.services.account.BittrexStreamingAccountService;
import info.bitrich.xchangestream.bittrex.services.marketdata.BittrexStreamingMarketDataService;
import info.bitrich.xchangestream.bittrex.services.trade.BittrexStreamingTradeService;
import info.bitrich.xchangestream.core.*;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.bittrex.service.BittrexMarketDataService;
import org.knowm.xchange.bittrex.service.BittrexTradeService;

public class BittrexStreamingExchange extends BittrexExchange implements StreamingExchange {

  // Bittrex WebSocket API endpoint
  private static final String API_BASE_URL = "https://socket-v3.bittrex.com/signalr";

  /** xchange-stream-bittrex services */
  private BittrexStreamingService bittrexStreamingService;

  private BittrexStreamingAccountService bittrexStreamingAccountService;
  private BittrexStreamingMarketDataService bittrexStreamingMarketDataService;
  private BittrexStreamingTradeService bittrexStreamingTradeService;

  @Override
  protected void initServices() {
    super.initServices();
    ExchangeSpecification exchangeSpecification = this.getExchangeSpecification();

    BittrexAccountService bittrexAccountService = (BittrexAccountService) this.getAccountService();
    BittrexMarketDataService bittrexMarketDataService =
        (BittrexMarketDataService) this.getMarketDataService();
    BittrexTradeService bittrexTradeService = (BittrexTradeService) this.getTradeService();
    bittrexStreamingService = new BittrexStreamingService(API_BASE_URL, exchangeSpecification);
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
    return this.bittrexStreamingService.disconnect();
  }

  @Override
  public boolean isAlive() {
    return bittrexStreamingService.isAlive();
  }

  public StreamingMarketDataService getStreamingMarketDataService() {
    return bittrexStreamingMarketDataService;
  }

  @Override
  public StreamingAccountService getStreamingAccountService() {
    return bittrexStreamingAccountService;
  }

  @Override
  public StreamingTradeService getStreamingTradeService() {
    return bittrexStreamingTradeService;
  }

  public void useCompressedMessages(boolean compressedMessages) {
    bittrexStreamingService.useCompressedMessages(compressedMessages);
  }
}
