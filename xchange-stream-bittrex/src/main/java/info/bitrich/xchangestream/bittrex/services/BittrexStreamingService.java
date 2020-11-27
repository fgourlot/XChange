package info.bitrich.xchangestream.bittrex.services;

import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingConnectionPool;
import info.bitrich.xchangestream.bittrex.connection.BittrexStreamingSubscription;
import io.reactivex.Completable;
import org.knowm.xchange.ExchangeSpecification;

public class BittrexStreamingService {

  // Careful, POOL_SIZE=10 caused problem on reconnection on all of them, for an unknown reasons
  public static final int POOL_SIZE = 5;

  private final BittrexStreamingConnectionPool connectionPool;

  public BittrexStreamingService(String apiUrl, ExchangeSpecification spec) {
    this.connectionPool =
        new BittrexStreamingConnectionPool(
            apiUrl, POOL_SIZE, spec.getApiKey(), spec.getSecretKey());
  }

  public void subscribeToChannelWithHandler(BittrexStreamingSubscription subscription) {
    this.connectionPool.subscribeToChannelWithHandler(subscription);
  }

  public Completable connect() {
    return this.connectionPool.connect();
  }

  public Completable disconnect() {
    return this.connectionPool.disconnect();
  }

  public boolean isAlive() {
    return this.connectionPool.isAlive();
  }

  public void useCompressedMessages(boolean compressedMessages) {
    throw new UnsupportedOperationException();
  }
}