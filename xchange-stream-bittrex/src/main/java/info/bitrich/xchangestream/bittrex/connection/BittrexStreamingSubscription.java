package info.bitrich.xchangestream.bittrex.connection;

import java.util.Arrays;
import java.util.Objects;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;

public class BittrexStreamingSubscription {

  private final String eventName;
  private final String[] channels;
  private final SubscriptionHandler1<String> handler;

  public BittrexStreamingSubscription(String eventName, String[] channels, SubscriptionHandler1<String> handler) {
    this.eventName = eventName;
    this.channels = channels;
    this.handler = handler;
  }

  public String getEventName() {
    return eventName;
  }

  public String[] getChannels() {
    return channels;
  }

  public SubscriptionHandler1<String> getHandler() {
    return handler;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BittrexStreamingSubscription that = (BittrexStreamingSubscription) o;
    return Arrays.equals(channels, that.channels)
        && Objects.equals(eventName, that.eventName)
        && Objects.equals(handler, that.handler);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(eventName, handler);
    result = 31 * result + Arrays.hashCode(channels);
    return result;
  }

  @Override
  public String toString() {
    return "Subscription{"
        + "eventName='"
        + eventName
        + '\''
        + ", channels="
        + Arrays.toString(channels)
        + ", handler="
        + handler
        + '}';
  }
}
