package info.bitrich.xchangestream.bittrex;

import com.github.signalr4j.client.hubs.HubConnection;
import com.github.signalr4j.client.hubs.HubProxy;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.knowm.xchange.ExchangeSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.UUID;

public class BittrexStreamingService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingService.class);

  private final ExchangeSpecification exchangeSpecification;

  private final HubConnection hubConnection;
  private final HubProxy hubProxy;

  public BittrexStreamingService(String apiUrl, ExchangeSpecification exchangeSpecification) {
    LOG.info("Initializing streaming service ...");
    this.exchangeSpecification = exchangeSpecification;
    hubConnection = new HubConnection(apiUrl);
    hubProxy = hubConnection.createHubProxy("c3");
    hubConnection.connected(this::connectedToWebSocket);
    LOG.info("Streaming service initialized...");
  }

  public io.reactivex.Completable connect() {
    LOG.info("Starting connection ...");
    return Completable.fromFuture(this.hubConnection.start());
  }

  public void disconnect() {
    LOG.info("Disconnecting ...");
    this.hubConnection.disconnect();
  }

  public void subscribeToChannelWithHandler(
      String[] channels, String eventName, SubscriptionHandler1<String> handler) {
    LOG.info("Subscribing ...");
    hubProxy.on(eventName, handler, String.class);
    hubProxy
        .invoke(Object.class, "Subscribe", (Object) channels)
        .onError(e -> LOG.error("Subscription error", e))
        .done(o -> LOG.info("Subscription success {}", o));
  }

  private void connectedToWebSocket() {
    this.setupAutoReAuthentication();
    this.authenticate();
  }

  private void authenticate() {
    LOG.info("Authenticating ...");
    Date date = new Date();
    Long ts = date.getTime();
    UUID uuid = UUID.randomUUID();
    String randomContent = ts.toString() + uuid.toString();

    try {
      String signedContent =
          EncryptionUtils.calculateHash(
              this.exchangeSpecification.getSecretKey(), randomContent, "HmacSHA512");
      hubProxy
          .invoke(
              Object.class,
              "Authenticate",
              this.exchangeSpecification.getApiKey(),
              ts,
              uuid,
              signedContent)
          .onError(error -> LOG.error("Error during Authentication", error))
          .done(obj -> LOG.info("Authentication success"));
    } catch (InvalidKeyException | NoSuchAlgorithmException e) {
      LOG.error("Could not authenticate", e);
    }
  }

  /** Auto-reauthenticate */
  private void setupAutoReAuthentication() {
    hubProxy.on(
        "authenticationExpiring",
        () -> {
          LOG.info("Authentication expiring, will reconnect ...");
          this.authenticate();
        });
  }
}
