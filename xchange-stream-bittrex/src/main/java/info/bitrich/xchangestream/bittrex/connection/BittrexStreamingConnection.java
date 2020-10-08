package info.bitrich.xchangestream.bittrex.connection;

import java.util.Arrays;
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

import com.github.signalr4j.client.ConnectionState;
import com.github.signalr4j.client.SignalRFuture;
import com.github.signalr4j.client.hubs.HubConnection;
import com.github.signalr4j.client.hubs.HubProxy;

import info.bitrich.xchangestream.bittrex.BittrexEncryptionUtils;
import io.reactivex.Completable;

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

  public BittrexStreamingConnection(String apiUrl, String apiKey, String secretKey, int id) {
    LOG.info("Initializing streaming service ... id={}", id);
    this.id = id;
    this.apiKey = apiKey;
    this.secretKey = secretKey;
    this.apiUrl = apiUrl;
    this.subscriptions = new HashSet<>();
    this.reconnecterTimer = new Timer();
    init();
    startReconnecter();
    LOG.info("Streaming service initialized... id={}", id);
  }

  private SignalRFuture<BittrexStreamingSocketResponse> authenticate() {
    LOG.info("Authenticating... id={}", id);
    Date date = new Date();
    Long ts = date.getTime();
    UUID uuid = UUID.randomUUID();
    String randomContent = ts.toString() + uuid.toString();
    try {
      String signedContent =
          BittrexEncryptionUtils.calculateHash(
              this.secretKey, randomContent, "HmacSHA512");
      return hubProxy
          .invoke(
              BittrexStreamingSocketResponse.class,
              "Authenticate",
              this.apiKey,
              ts,
              uuid,
              signedContent)
          .onError(error -> LOG.error("Error during Authentication", error))
          .done(response -> LOG.info("Authentication success id={}, {}", id, response));
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

  public Completable connect() {
    LOG.info("Starting connection... id={}", id);
    return Completable.fromFuture(this.hubConnection.start());
  }

  public Completable disconnect() {
    LOG.info("Disconnecting... id={}", id);
    this.reconnecterTimer.cancel();
    this.hubConnection.disconnect();
    return Completable.complete();
  }

  public boolean isAlive() {
    return ConnectionState.Connected.equals(hubConnection.getState()) || ConnectionState.Connecting.equals(hubConnection.getState());
  }

  private void startReconnecter() {
    this.reconnecterTimer.cancel();
    this.reconnecterTimer = new Timer();
    reconnecterTimer.scheduleAtFixedRate(
        new TimerTask() {
          public void run() {
            if (!ConnectionState.Connected.equals(hubConnection.getState())) {
              LOG.info("Reconnecting... id={}", id);
              init();
              connect().blockingAwait();
              LOG.info("Reconnected! id={}", id);
              subscriptions.forEach(subscription -> subscribeToChannelWithHandler(subscription, true));
            }
          }
        },
        60_000,
        1_000);
  }

  public void subscribeToChannelWithHandler(BittrexStreamingSubscription subscription, boolean needAuthentication) {
    if (needAuthentication) {
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
    LOG.info("Subscribing to {}", subscription);
    hubProxy.on(subscription.getEventName(), subscription.getHandler(), String.class);
    hubProxy
        .invoke(BittrexStreamingSocketResponse[].class, "Subscribe", (Object) subscription.getChannels())
        .onError(e -> LOG.error("Subscription error", e))
        .done(
            response -> {
              LOG.info(
                  "Subscription success {}",
                  Arrays.stream(response)
                      .map(BittrexStreamingSocketResponse::toString)
                      .collect(Collectors.joining(";")));
              subscriptions.add(subscription);
            });
  }

  /** Auto-reauthenticate */
  private void setupAutoReAuthentication() {
    hubProxy.on(
        "authenticationExpiring",
        () -> {
          LOG.info("Authentication expiring, reauthenticating... id={}", id);
          this.authenticate();
        });
  }

}
