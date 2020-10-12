package info.bitrich.xchangestream.bittrex.connection;

import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import java.util.Arrays;
import java.util.Objects;

public class BittrexStreamingSubscription {

  private final String eventName;
  private final String[] channels;
  private final boolean needAuthentication;
  private final SubscriptionHandler1<String> handler;

  public BittrexStreamingSubscription(String eventName, String[] channels, boolean needAuthentication, SubscriptionHandler1<String> handler) {
    this.eventName = eventName;
    this.channels = channels;
    this.needAuthentication = needAuthentication;
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

  public boolean isNeedAuthentication() {
    return needAuthentication;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BittrexStreamingSubscription that = (BittrexStreamingSubscription) o;
    return needAuthentication == that.needAuthentication
        && Objects.equals(eventName, that.eventName)
        && Arrays.equals(channels, that.channels)
        && Objects.equals(handler, that.handler);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(eventName, needAuthentication, handler);
    result = 31 * result + Arrays.hashCode(channels);
    return result;
  }

  @Override
  public String toString() {
    return "BittrexStreamingSubscription{"
        + "eventName='"
        + eventName
        + '\''
        + ", channels="
        + Arrays.toString(channels)
        + ", needAuthentication="
        + needAuthentication
        + ", handler="
        + handler
        + '}';
  }
}
