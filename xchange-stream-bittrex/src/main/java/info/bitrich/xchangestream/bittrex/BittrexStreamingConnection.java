package info.bitrich.xchangestream.bittrex;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.knowm.xchange.ExchangeSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.signalr4j.client.ConnectionState;
import com.github.signalr4j.client.SignalRFuture;
import com.github.signalr4j.client.hubs.HubConnection;
import com.github.signalr4j.client.hubs.HubProxy;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;

import io.reactivex.Completable;

public class BittrexStreamingConnection {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingConnection.class);
  public static final String COULD_NOT_AUTHENTICATE_ERROR_MESSAGE = "Could not authenticate";

  private final ExchangeSpecification exchangeSpecification;

  private HubConnection hubConnection;
  private HubProxy hubProxy;
  private final String apiUrl;
  private final Set<Subscription> subscriptions;
  private Timer reconnecterTimer;

  public BittrexStreamingConnection(String apiUrl, ExchangeSpecification exchangeSpecification) {
    LOG.info("Initializing streaming service ...");
    this.exchangeSpecification = exchangeSpecification;
    this.apiUrl = apiUrl;
    this.subscriptions = new HashSet<>();
    this.reconnecterTimer = new Timer();
    init();
    startReconnecter();
    LOG.info("Streaming service initialized...");
  }

  private SignalRFuture<SocketResponse> authenticate() {
    LOG.info("Authenticating...");
    Date date = new Date();
    Long ts = date.getTime();
    UUID uuid = UUID.randomUUID();
    String randomContent = ts.toString() + uuid.toString();
    try {
      String signedContent =
          BittrexEncryptionUtils.calculateHash(
              this.exchangeSpecification.getSecretKey(), randomContent, "HmacSHA512");
      return hubProxy
          .invoke(
              SocketResponse.class,
              "Authenticate",
              this.exchangeSpecification.getApiKey(),
              ts,
              uuid,
              signedContent)
          .onError(error -> LOG.error("Error during Authentication", error))
          .done(response -> LOG.info("Authentication success {}", response));
    } catch (Exception e) {
      LOG.error(COULD_NOT_AUTHENTICATE_ERROR_MESSAGE, e);
    }
    return null;
  }

  private void init() {
    if (hubConnection != null) {
      hubConnection.disconnect();
    }
    hubConnection = new HubConnection(this.apiUrl);
    hubProxy = hubConnection.createHubProxy("c3");
    hubConnection.connected(this::setupAutoReAuthentication);
  }

  private Completable connect() {
    LOG.info("Starting connection ...");
    return Completable.fromFuture(this.hubConnection.start());
  }

  public void disconnect() {
    LOG.info("Disconnecting ...");
    this.hubConnection.disconnect();
  }

  public boolean isAlive() {
    return !hubConnection.getState().equals(ConnectionState.Disconnected);
  }

  private void startReconnecter() {
    this.reconnecterTimer.cancel();
    this.reconnecterTimer = new Timer();
    reconnecterTimer.scheduleAtFixedRate(
        new TimerTask() {
          public void run() {
            if (!ConnectionState.Connected.equals(hubConnection.getState())) {
              LOG.info("Reconnecting...");
              init();
              connect().blockingAwait();
              LOG.info("Reconnected!");
              subscriptions.forEach(subscription -> subscribeToChannelWithHandler(subscription, true));
            }
          }
        },
        60_000,
        10_000);
  }

  public void subscribeToChannelWithHandler(Subscription subscription, boolean needAuthentication) {
    if (needAuthentication) {
      try {
        SignalRFuture<SocketResponse> authenticateFuture = this.authenticate();
        if (authenticateFuture != null) {
          authenticateFuture.get();
        }
      } catch (InterruptedException ie) {
        LOG.error("Could not authenticate", ie);
        Thread.currentThread().interrupt();
      } catch (ExecutionException ee) {
        LOG.error("Could not authenticate", ee);
      }
    }
    LOG.info("Subscribing to {}", subscription);
    hubProxy.on(subscription.getEventName(), subscription.getHandler(), String.class);
    hubProxy
        .invoke(SocketResponse[].class, "Subscribe", (Object) subscription.getChannels())
        .onError(e -> LOG.error("Subscription error", e))
        .done(
            response -> {
              LOG.info(
                  "Subscription success {}",
                  Arrays.stream(response)
                      .map(SocketResponse::toString)
                      .collect(Collectors.joining(";")));
              subscriptions.add(subscription);
            });
  }

  /** Auto-reauthenticate */
  private void setupAutoReAuthentication() {
    hubProxy.on(
        "authenticationExpiring",
        () -> {
          LOG.info("Authentication expiring, reauthenticating...");
          this.authenticate();
        });
  }

  public void useCompressedMessages(boolean compressedMessages) {
    throw new UnsupportedOperationException();
  }

  static class SocketResponse {
    private final Boolean Success;
    private final String ErrorCode;

    public SocketResponse(Boolean success, String error) {
      Success = success;
      ErrorCode = error;
    }

    @Override
    public String toString() {
      return "SocketResponse{" + "Success=" + Success + ", ErrorCode='" + ErrorCode + '\'' + '}';
    }
  }

  static class Subscription {
    private final String eventName;
    private final String[] channels;
    private final SubscriptionHandler1<String> handler;

    public Subscription(String eventName, String[] channels, SubscriptionHandler1<String> handler) {
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
      Subscription that = (Subscription) o;
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
      return "Subscription{" +
          "eventName='" + eventName + '\'' +
          ", channels=" + Arrays.toString(channels) +
          ", handler=" + handler +
          '}';
    }

  }
}
