package info.bitrich.xchangestream.bittrex.connection;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.signalr4j.client.ConnectionState;
import com.github.signalr4j.client.SignalRFuture;
import com.github.signalr4j.client.hubs.HubConnection;
import com.github.signalr4j.client.hubs.HubProxy;

import info.bitrich.xchangestream.bittrex.BittrexEncryptionUtils;
import io.reactivex.Completable;

public class BittrexStreamingConnection {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingConnection.class);
  private static final AtomicInteger ID_COUNTER = new AtomicInteger();
  public static final String COULD_NOT_AUTHENTICATE_ERROR_MESSAGE = "Could not authenticate";

  private final String apiKey;
  private final String secretKey;
  private final int id;
  private HubConnection hubConnection;
  private HubProxy hubProxy;
  private final String apiUrl;
  private final Set<BittrexStreamingSubscription> subscriptions;
  private Timer reconnecterTimer;
  private boolean authenticating;

  public BittrexStreamingConnection(String apiUrl, String apiKey, String secretKey) {
    this.id = ID_COUNTER.getAndIncrement();
    LOG.info("[ConnId={}] Initializing streaming service ...", id);
    this.apiKey = apiKey;
    this.secretKey = secretKey;
    this.apiUrl = apiUrl;
    this.subscriptions = new HashSet<>();
    this.authenticating = false;
    initConnection();
    LOG.info("[ConnId={}] Streaming service initialized...", id);
  }

  private SignalRFuture<BittrexStreamingSocketResponse> authenticate() {
    if (secretKey == null || apiKey == null) {
      return null;
    }
    LOG.info("[ConnId={}] Authenticating...", id);
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
              BittrexStreamingSocketResponse.class,
              "Authenticate",
              apiKey,
              ts,
              uuid,
              signedContent)
          .onError(error -> LOG.error("Authentication error", error))
          .done(response -> LOG.info("[ConnId={}] Authentication success: {}", id, response));
    } catch (Exception e) {
      LOG.error(COULD_NOT_AUTHENTICATE_ERROR_MESSAGE, e);
    }
    return null;
  }

  private void initConnection() {
    authenticating = false;
    subscriptions.forEach(sub -> hubProxy.removeSubscription(sub.getEventName()));
    if (hubConnection != null) {
      hubConnection.disconnect();
    }
    hubConnection = new HubConnection(apiUrl);
    hubProxy = hubConnection.createHubProxy("c3");
    hubConnection.connected(this::onConnection);
  }

  public Completable connect() {
    LOG.info("[ConnId={}] Starting connection...", id);
    return Completable.fromFuture(hubConnection.start());
  }

  public Completable disconnect() {
    LOG.info("[ConnId={}] Disconnecting...", id);
    authenticating = false;
    reconnecterTimer.cancel();
    hubConnection.disconnect();
    return Completable.complete();
  }

  public boolean isAlive() {
    return ConnectionState.Connected.equals(hubConnection.getState())
        || ConnectionState.Connecting.equals(hubConnection.getState());
  }

  private void startReconnecter() {
    if (reconnecterTimer != null) {
      reconnecterTimer.cancel();
    }
    reconnecterTimer = new Timer();
    reconnecterTimer.schedule(
        new TimerTask() {
          public void run() {
            try {
              if (!ConnectionState.Connected.equals(hubConnection.getState())) {
                LOG.info(
                    "[ConnId={}] Initiating reconnection, state is {}",
                    id,
                    hubConnection.getState());
                reconnectAndSubscribe();
              }
            } catch (Exception e) {
              LOG.error("[ConnId={}] Reconnection error: {}", id, e);
            }
          }
        },
        5_000,
        1_000);
  }

  private void reconnectAndSubscribe() {
    LOG.info("[ConnId={}] Reconnecting...", id);
    initConnection();
    connect().blockingAwait();
    LOG.info("[ConnId={}] Reconnected!", id);
    String events =
        subscriptions.stream()
            .map(BittrexStreamingSubscription::getEventName)
            .collect(Collectors.joining(", "));
    LOG.info("[ConnId={}] Subscribing to events {}...", id, events);
    subscriptions.forEach(this::subscribeToChannelWithHandler);
    LOG.info("[ConnId={}] Events {} subscribed!", id, events);
  }

  public void subscribeToChannelWithHandler(BittrexStreamingSubscription subscription) {
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
    LOG.info("[ConnId={}] Subscribing to {}", id, subscription);
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
                  "[ConnId={}] Subscription success to {}: {}",
                  id,
                  subscription,
                  Arrays.stream(response)
                      .map(BittrexStreamingSocketResponse::toString)
                      .collect(Collectors.joining(";")));
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
    startReconnecter();
  }

  /** Auto-reauthenticate */
  private void setupAutoReAuthentication() {
    hubProxy.on(
        "authenticationExpiring",
        () -> {
          LOG.info("[ConnId={}] Authentication expiring, reauthenticating...", id);
          authenticate();
        });
  }
}
