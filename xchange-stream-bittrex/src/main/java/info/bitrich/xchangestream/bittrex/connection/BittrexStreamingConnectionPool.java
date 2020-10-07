package info.bitrich.xchangestream.bittrex.connection;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import info.bitrich.xchangestream.core.ProductSubscription;
import io.reactivex.Completable;

public class BittrexStreamingConnectionPool {

  private final List<BittrexStreamingConnection> bittrexStreamingConnections;

  public BittrexStreamingConnectionPool(String url, int poolSize, String apiKey, String secretKey) {
    bittrexStreamingConnections =
        IntStream.range(0, poolSize)
            .mapToObj(i -> new BittrexStreamingConnection(url, apiKey, secretKey, i))
            .collect(Collectors.toList());
  }

  public io.reactivex.Completable connect() {
    return Completable.mergeArray(
        bittrexStreamingConnections.stream()
            .map(BittrexStreamingConnection::connect)
            .toArray(Completable[]::new));
  }

  public io.reactivex.Completable disconnect() {
    return Completable.mergeArray(
        bittrexStreamingConnections.stream()
                                   .map(BittrexStreamingConnection::disconnect)
                                   .toArray(Completable[]::new));
  }

  public void subscribeToChannelWithHandler(BittrexStreamingSubscription subscription, boolean needAuthentication) {
    bittrexStreamingConnections.forEach(connection -> connection.subscribeToChannelWithHandler(subscription, needAuthentication));

  }

  public boolean isAlive() {
    return bittrexStreamingConnections.stream().anyMatch(BittrexStreamingConnection::isAlive);
  }

}
