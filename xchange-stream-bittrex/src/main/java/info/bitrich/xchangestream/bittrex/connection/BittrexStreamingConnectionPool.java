package info.bitrich.xchangestream.bittrex.connection;

import io.reactivex.Completable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BittrexStreamingConnectionPool {

  private final List<BittrexStreamingConnection> bittrexStreamingConnections;

  // Careful, POOL_SIZE=10 caused problem on reconnection on all of them, for an unknown reason
  public BittrexStreamingConnectionPool(String url, int poolSize, String apiKey, String secretKey) {
    bittrexStreamingConnections =
        IntStream.range(0, poolSize)
            .mapToObj(index -> new BittrexStreamingConnection(url, apiKey, secretKey))
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

  public void subscribeToChannelWithHandler(BittrexStreamingSubscription subscription) {
    bittrexStreamingConnections.forEach(
        connection -> connection.subscribeToChannelWithHandler(subscription));
  }

  public boolean isAlive() {
    return bittrexStreamingConnections.stream().anyMatch(BittrexStreamingConnection::isAlive);
  }
}
