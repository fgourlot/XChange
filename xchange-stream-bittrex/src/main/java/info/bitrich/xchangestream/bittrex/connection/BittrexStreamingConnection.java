package info.bitrich.xchangestream.bittrex.connection;

import com.github.signalr4j.client.ConnectionState;
import com.github.signalr4j.client.SignalRFuture;
import com.github.signalr4j.client.hubs.HubConnection;
import com.github.signalr4j.client.hubs.HubProxy;
import info.bitrich.xchangestream.bittrex.BittrexEncryptionUtils;
import io.reactivex.Completable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingConnection {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingConnection.class);
  private static final AtomicInteger ID_COUNTER = new AtomicInteger();
  private static final String COULD_NOT_AUTHENTICATE_ERROR_MESSAGE = "Could not authenticate";

  private final String apiKey;
  private final String secretKey;
  private final int id;
  private HubConnection hubConnection;
  private HubProxy hubProxy;
  private final String apiUrl;
  private final Set<BittrexStreamingSubscription> subscriptions;
  private boolean authenticating;

  public BittrexStreamingConnection(String apiUrl, String apiKey, String secretKey) {
    this.id = ID_COUNTER.getAndIncrement();
    LOG.info("[ConnId={}] Initializing streaming service ...", id);
    this.apiKey = apiKey;
    this.secretKey = secretKey;
    this.apiUrl = apiUrl;
    this.subscriptions = new HashSet<>();
    this.authenticating = false;
    initConnectionConfiguration();
    LOG.info("[ConnId={}] Streaming service initialized...", id);
  }

  private SignalRFuture<BittrexStreamingSocketResponse> authenticate() {
    if (secretKey == null || apiKey == null) {
      return null;
    }
    LOG.debug("[ConnId={}] Authenticating...", id);
    authenticating = true;
    Date date = new Date();
    Long ts = date.getTime();
    UUID uuid = UUID.randomUUID();
    String randomContent = ts.toString() + uuid.toString();
    try {
      String signedContent =
          BittrexEncryptionUtils.calculateHash(secretKey, randomContent, "HmacSHA512");
      return hubProxy
          .invoke(
              BittrexStreamingSocketResponse.class, "Authenticate", apiKey, ts, uuid, signedContent)
          .onError(error -> LOG.error("[ConnId={}] Authentication error: {}", id, error))
          .done(response -> LOG.info("[ConnId={}] (re)authenticated!", id));
    } catch (Exception e) {
      LOG.error(COULD_NOT_AUTHENTICATE_ERROR_MESSAGE, e);
    }
    return null;
  }

  private synchronized void initConnectionConfiguration() {
    authenticating = false;
    if (hubProxy != null) {
      subscriptions.forEach(sub -> hubProxy.removeSubscription(sub.getEventName()));
    }
    disconnect().blockingAwait();
    hubConnection = new HubConnection(apiUrl);
    hubConnection.stateChanged(
        (oldState, newState) -> {
          if (ConnectionState.Connected.equals(oldState)) {
            LOG.info(
                "[ConnId={}] Initiating reconnection because state changed from '{}' to '{}'",
                id,
                oldState,
                newState);
            reconnectAndSubscribe();
          }
        });
    hubConnection.connectionSlow(
        () -> {
          LOG.error("[ConnId={}] Connection slow detected!", id);
          reconnectAndSubscribe();
        });
    hubConnection.error(
        e -> {
          LOG.error("[ConnId={}] Connection error detected!", id, e);
          reconnectAndSubscribe();
        });
    hubConnection.closed(
        () -> {
          LOG.error("[ConnId={}] Connection closed detected!", id);
          reconnectAndSubscribe();
        });
    hubConnection.connected(this::onConnection);
    hubProxy = hubConnection.createHubProxy("c3");
  }

  public Completable connect() {
    LOG.info("[ConnId={}] Starting connection...", id);
    return Completable.fromFuture(hubConnection.start());
  }

  public Completable disconnect() {
    authenticating = false;
    if (hubConnection != null) {
      LOG.info("[ConnId={}] Disconnecting...", id);
      hubConnection.stateChanged((oldState, newState) -> {});
      hubConnection.connectionSlow(() -> {});
      hubConnection.error(e -> {});
      hubConnection.closed(() -> {});
      hubConnection.disconnect();
    }
    return Completable.complete();
  }

  public boolean isAlive() {
    return ConnectionState.Connected.equals(hubConnection.getState())
        || ConnectionState.Connecting.equals(hubConnection.getState());
  }

  private synchronized void reconnectAndSubscribe() {
    try {
      LOG.info("[ConnId={}] Reconnecting...", id);
      initConnectionConfiguration();
      connect().blockingAwait();
      LOG.info("[ConnId={}] Reconnected!", id);
      String events =
          subscriptions.stream()
              .map(BittrexStreamingSubscription::getEventName)
              .collect(Collectors.joining(", "));
      LOG.info("[ConnId={}] Subscribing to events {}...", id, events);
      subscriptions.forEach(this::subscribeToChannelWithHandler);
    } catch (Exception e) {
      LOG.error("[ConnId={}] Reconnection error: {}", id, e);
      reconnectAndSubscribe();
    }
  }

  public synchronized void subscribeToChannelWithHandler(
      BittrexStreamingSubscription subscription) {
    CountDownLatch latch = new CountDownLatch(1);
    if (!authenticating && subscription.isNeedAuthentication()) {
      try {
        SignalRFuture<BittrexStreamingSocketResponse> authenticateFuture = authenticate();
        if (authenticateFuture != null) {
          authenticateFuture.get();
        }
      } catch (InterruptedException ie) {
        LOG.error(COULD_NOT_AUTHENTICATE_ERROR_MESSAGE, ie);
        Thread.currentThread().interrupt();
      } catch (ExecutionException ee) {
        LOG.error(COULD_NOT_AUTHENTICATE_ERROR_MESSAGE, ee);
      }
    }
    LOG.debug("[ConnId={}] Subscribing to event {}", id, subscription.getEventName());
    hubProxy.on(subscription.getEventName(), subscription.getHandler(), String.class);
    hubProxy
        .invoke(
            BittrexStreamingSocketResponse[].class,
            "Subscribe",
            (Object) subscription.getChannels())
        .onError(
            e -> {
              LOG.error(
                  "[ConnId={}] Subscription error to {}: {}", id, subscription, e.getMessage());
              latch.countDown();
            })
        .done(
            response -> {
              LOG.info(
                  "[ConnId={}] Subscription success to event {}", id, subscription.getEventName());
              latch.countDown();
              subscriptions.add(subscription);
            });
    try {
      latch.await();
    } catch (InterruptedException e) {
      LOG.error("[ConnId={}] Error subscribing: {}", id, e);
      Thread.currentThread().interrupt();
    }
  }

  private void onConnection() {
    setupAutoReAuthentication();
  }

  /** Auto-reauthenticate */
  private void setupAutoReAuthentication() {
    hubProxy.on(
        "authenticationExpiring",
        () -> {
          LOG.debug("[ConnId={}] Authentication expiring, reauthenticating...", id);
          authenticate();
        });
  }
}
