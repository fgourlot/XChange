package info.bitrich.xchangestream.bittrex.connection;

import com.github.signalr4j.client.ConnectionState;
import com.github.signalr4j.client.SignalRFuture;
import com.github.signalr4j.client.hubs.HubConnection;
import com.github.signalr4j.client.hubs.HubProxy;
import info.bitrich.xchangestream.bittrex.services.utils.BittrexStreamingEncryptionUtils;
import io.reactivex.Completable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class BittrexStreamingConnection {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingConnection.class);
  private static final AtomicInteger ID_COUNTER = new AtomicInteger();
  private static final String COULD_NOT_AUTHENTICATE_ERROR_MESSAGE = "Could not authenticate";
  private static final String AUTHENTICATION_EXPIRING_EVENT = "authenticationExpiring";

  private final String apiKey;
  private final String secretKey;
  private final int id;
  private HubConnection hubConnection;
  private HubProxy hubProxy;
  private final String apiUrl;
  private final Set<BittrexStreamingSubscription> subscriptions;
  private boolean authenticating;

  private final ExecutorService reconnectAndSubscribeExecutor;
  private final List<Future<?>> reconnectTasks;

  public BittrexStreamingConnection(String apiUrl, String apiKey, String secretKey) {
    this.id = ID_COUNTER.getAndIncrement();
    LOG.info("[ConnId={}] Initializing streaming service ...", id);
    this.apiKey = apiKey;
    this.secretKey = secretKey;
    this.apiUrl = apiUrl;
    this.subscriptions = new HashSet<>();
    this.authenticating = false;
    this.reconnectAndSubscribeExecutor = Executors.newCachedThreadPool();
    this.reconnectTasks = Collections.synchronizedList(new ArrayList<>(1));
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
          BittrexStreamingEncryptionUtils.calculateHash(secretKey, randomContent, "HmacSHA512");
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
      hubProxy.removeSubscription(AUTHENTICATION_EXPIRING_EVENT);
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
    hubConnection.setReconnectOnError(false);
    hubConnection.reconnecting(() -> {});
    hubProxy = hubConnection.createHubProxy("c3");
    hubConnection.connected(this::onConnection);
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
      // We don't really care if it works, and we don't want to be stuck in case it doesn't...
      ExecutorService discExecutor = Executors.newFixedThreadPool(1);
      discExecutor.execute(() -> hubConnection.disconnect());
      discExecutor.shutdown();
      try {
        boolean disconnected = discExecutor.awaitTermination(5, TimeUnit.SECONDS);
        if (disconnected) {
          LOG.info("[ConnId={}] Disconnected!", id);
        } else {
          LOG.info("[ConnId={}] Disconnection failed (timeout)!", id);
          discExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        LOG.error("Error disconnecting!", e);
        Thread.currentThread().interrupt();
      }
    }
    return Completable.complete();
  }

  public boolean isAlive() {
    return ConnectionState.Connected.equals(hubConnection.getState())
        || ConnectionState.Connecting.equals(hubConnection.getState());
  }

  private void reconnectAndSubscribe() {
    reconnectTasks.removeIf(Future::isDone);
    if (!reconnectTasks.isEmpty()) {
      boolean cancelled = reconnectTasks.stream().allMatch(task -> task.cancel(true));
      if (!cancelled) {
        LOG.warn(
            "[ConnId={}] Could not cancel at least running reconnect task ({} tasks running)",
            id,
            reconnectTasks.size());
      }
    }
    Future<?> task =
        reconnectAndSubscribeExecutor.submit(
            () -> {
              boolean success = false;
              LOG.info("[ConnId={}] Reconnecting...", id);
              try {
                initConnectionConfiguration();
                connect().blockingAwait();
                LOG.info("[ConnId={}] Reconnected!", id);
                String events =
                    subscriptions.stream()
                        .map(BittrexStreamingSubscription::getEventName)
                        .collect(Collectors.joining(", "));
                LOG.info("[ConnId={}] Subscribing to events {}...", id, events);
                success = subscriptions.stream().allMatch(this::subscribeToChannelWithHandler);
              } catch (Exception e) {
                LOG.error("[ConnId={}] Reconnection error!", id, e);
              }
              if (!success) {
                reconnectAndSubscribe();
              } else {
                LOG.info("[ConnId={}] Reconnection success!", id);
              }
            });
    reconnectTasks.add(task);
    try {
      task.get();
      reconnectTasks.remove(task);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public synchronized boolean subscribeToChannelWithHandler(
      BittrexStreamingSubscription subscription) {
    AtomicBoolean success = new AtomicBoolean(false);
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
              success.set(false);
              latch.countDown();
            })
        .done(
            response -> {
              LOG.info(
                  "[ConnId={}] Subscription success to event {}", id, subscription.getEventName());
              success.set(
                  Arrays.stream(response).allMatch(BittrexStreamingSocketResponse::getSuccess));
              latch.countDown();
              subscriptions.add(subscription);
            });
    try {
      latch.await();
    } catch (InterruptedException e) {
      LOG.error("[ConnId={}] Error subscribing: {}", id, e);
      Thread.currentThread().interrupt();
    }
    return success.get();
  }

  private void onConnection() {
    setupAutoReAuthentication();
  }

  /** Auto-reauthenticate */
  private void setupAutoReAuthentication() {
    hubProxy.on(
        AUTHENTICATION_EXPIRING_EVENT,
        () -> {
          LOG.debug("[ConnId={}] Authentication expiring, reauthenticating...", id);
          authenticate();
        });
  }
}
