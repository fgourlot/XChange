package info.bitrich.xchangestream.bittrex;

import com.github.signalr4j.client.hubs.HubConnection;
import com.github.signalr4j.client.hubs.HubProxy;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import io.reactivex.Completable;
import io.reactivex.Observable;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.UUID;
import org.knowm.xchange.ExchangeSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BittrexStreamingService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingService.class);

  private ExchangeSpecification exchangeSpecification;

  private HubConnection _hubConnection;
  private HubProxy _hubProxy;

  public BittrexStreamingService(String apiUrl, ExchangeSpecification exchangeSpecification) {
    this.exchangeSpecification = exchangeSpecification;
    _hubConnection = new HubConnection(apiUrl);
    _hubProxy = _hubConnection.createHubProxy("c3");
    _hubConnection.connected(this::connectedToWebSocket);
  }

  public io.reactivex.Completable connect() {
    LOG.info("Starting connection ...");
    return Completable.fromFuture(this._hubConnection.start());
  }

  public void disconnect() {
    LOG.info("Disconnecting ...");
    this._hubConnection.disconnect();
  }

  public Observable subscribeToChannels(String[] channels) {
    return Observable.fromFuture(
        _hubProxy
            .invoke(Object.class, "Subscribe", (Object) channels)
            .onError(
                e -> {
                  LOG.error("Error subscribe {}", e);
                })
            .done(
                o -> {
                  LOG.info("Success subscribe {}", o);
                }));
  }

  public void setHandler(String eventName, SubscriptionHandler1 handler) {
    _hubProxy.on(eventName, handler, String.class);
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
      _hubProxy
          .invoke(
              Object.class,
              "Authenticate",
              this.exchangeSpecification.getApiKey(),
              ts,
              uuid,
              signedContent)
          .onError(
              error -> {
                LOG.error("Error during Authentication {}", error);
              })
          .done(
              obj -> {
                LOG.info("Authentication success {}");
              });
    } catch (InvalidKeyException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  /** Auto-reauthenticate */
  private void setupAutoReAuthentication() {
    _hubProxy.on(
        "authenticationExpiring",
        () -> {
          LOG.info("Authentication expiring, will reconnect ...");
          this.authenticate();
        });
  }
}
