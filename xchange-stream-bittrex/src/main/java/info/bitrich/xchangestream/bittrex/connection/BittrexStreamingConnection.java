package info.bitrich.xchangestream.bittrex.connection;

import com.github.signalr4j.client.ConnectionState;
import com.github.signalr4j.client.SignalRFuture;
import com.github.signalr4j.client.hubs.HubConnection;
import com.github.signalr4j.client.hubs.HubProxy;
import info.bitrich.xchangestream.bittrex.BittrexEncryptionUtils;
import io.reactivex.Completable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingConnection {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingConnection.class);
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
  private final Object authLock = new Object();

  public BittrexStreamingConnection(String apiUrl, String apiKey, String secretKey, int id) {
    LOG.info("[ConnId={}] Initializing streaming service ...", id);
    this.id = id;
    this.apiKey = apiKey;
    this.secretKey = secretKey;
    this.apiUrl = apiUrl;
    this.subscriptions = new HashSet<>();
    this.authenticating = false;
    initConnection();
    LOG.info("[ConnId={}] Streaming service initialized...", id);
  }

  private SignalRFuture<BittrexStreamingSocketResponse> authenticate() {
    LOG.info("[ConnId={}] Authenticating...", id);
    this.authenticating = true;
    Date date = new Date();
    Long ts = date.getTime();
    UUID uuid = UUID.randomUUID();
    String randomContent = ts.toString() + uuid.toString();
    try {
      String signedContent =
          BittrexEncryptionUtils.calculateHash(this.secretKey, randomContent, "HmacSHA512");
      return hubProxy
          .invoke(
              BittrexStreamingSocketResponse.class,
              "Authenticate",
              this.apiKey,
              ts,
              uuid,
              signedContent)
          .onError(
              error -> {
                LOG.error("Authentication error", error);
                authenticate();
              })
          .done(response -> LOG.info("[ConnId={}] Authentication success: {}", id, response));
    } catch (Exception e) {
      LOG.error(COULD_NOT_AUTHENTICATE_ERROR_MESSAGE, e);
    }
    return null;
  }

  private void initConnection() {
    this.authenticating = false;
    if (hubConnection != null) {
      this.hubConnection.disconnect();
    }
    this.hubConnection = new HubConnection(this.apiUrl);
    this.hubProxy = hubConnection.createHubProxy("c3");
    this.hubConnection.connected(this::onConnection);
  }

  public Completable connect() {
    LOG.info("[ConnId={}] Starting connection...", id);
    return Completable.fromFuture(this.hubConnection.start());
  }

  public Completable disconnect() {
    LOG.info("[ConnId={}] Disconnecting...", id);
    this.authenticating = false;
    this.reconnecterTimer.cancel();
    this.hubConnection.disconnect();
    return Completable.complete();
  }

  public boolean isAlive() {
    return ConnectionState.Connected.equals(hubConnection.getState())
        || ConnectionState.Connecting.equals(hubConnection.getState());
  }

  private void startReconnecter() {
    if (this.reconnecterTimer != null) {
      this.reconnecterTimer.cancel();
    }
    this.reconnecterTimer = new Timer();
    this.reconnecterTimer.schedule(
        new TimerTask() {
          public void run() {
            try {
              if (!ConnectionState.Connected.equals(hubConnection.getState())) {
                reconnectAndSubscribe(subscriptions);
              }
            } catch (Exception e) {
              LOG.error("[ConnId={}] Reconnection error: {}", id, e.getMessage());
            }
          }
        },
        0,
        1_000);
  }

  private void reconnectAndSubscribe(Collection<BittrexStreamingSubscription> subscriptions) {
    LOG.info("[ConnId={}] Reconnecting...", id);
    initConnection();
    connect().blockingAwait();
    LOG.info("[ConnId={}] Reconnected!", id);
    subscriptions.forEach(this::subscribeToChannelWithHandler);
  }

  public void subscribeToChannelWithHandler(BittrexStreamingSubscription subscription) {
    synchronized (authLock) {
      if (!this.authenticating && subscription.isNeedAuthentication()) {
        try {
          SignalRFuture<BittrexStreamingSocketResponse> authenticateFuture = this.authenticate();
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
    }
    LOG.info("[ConnId= {}] Subscribing to {}", id, subscription);
    hubProxy.on(subscription.getEventName(), subscription.getHandler(), String.class);
    hubProxy
        .invoke(
            BittrexStreamingSocketResponse[].class,
            "Subscribe",
            (Object) subscription.getChannels())
        .onError(
            e -> {
              LOG.error("[ConnId={}] Subscription error to {}: {}", id, subscription, e);
              subscribeToChannelWithHandler(subscription);
            })
        .done(
            response -> {
              LOG.info(
                  "[ConnId={}] Subscription success to {}:{}",
                  id,
                  subscription,
                  Arrays.stream(response)
                      .map(BittrexStreamingSocketResponse::toString)
                      .collect(Collectors.joining(";")));
              subscriptions.add(subscription);
            });
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
          this.authenticate();
        });
  }
}
