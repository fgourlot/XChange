package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.dto.BittrexOrder;
import info.bitrich.xchangestream.core.StreamingTradeService;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.knowm.xchange.bittrex.BittrexUtils;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.UserTrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BittrexStreamingTradeService implements StreamingTradeService {
  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingTradeService.class);

  private final BittrexStreamingService service;

  ObjectMapper objectMapper;

  public BittrexStreamingTradeService(BittrexStreamingService service) {
    this.service = service;
    objectMapper = new ObjectMapper();
  }

  @Override
  public Observable<Order> getOrderChanges(CurrencyPair currencyPair, Object... args) {
    return null;
  }

  @Override
  public Observable<UserTrade> getUserTrades(CurrencyPair currencyPair, Object... args) {

    // create result Observable
    Observable<UserTrade> obs =
        new Observable<>() {
          @Override
          protected void subscribeActual(Observer<? super UserTrade> observer) {
            // create handler for `balance` messages
            SubscriptionHandler1<String> orderHandler =
                message -> {
                  LOG.debug("Incoming order message : {}", message);
                  try {
                    String decompressedMessage = EncryptionUtility.decompress(message);
                    LOG.debug("Decompressed order message : {}", decompressedMessage);
                    // parse JSON to Object
                    BittrexOrder bittrexOrder =
                        objectMapper.readValue(decompressedMessage, BittrexOrder.class);
                    UserTrade userTrade = bittrexOrderToUserTrade(bittrexOrder);
                    LOG.debug(
                        "Emitting Order with ID {} for operation {} {} on price {} for amount {}",
                        userTrade.getOrderId(),
                        userTrade.getType(),
                        userTrade.getCurrencyPair(),
                        userTrade.getPrice(),
                        userTrade.getOriginalAmount());
                    observer.onNext(userTrade);

                  } catch (IOException e) {
                    LOG.error("Error while receiving and treating order message", e);
                    throw new RuntimeException(e);
                  }
                };
            service.setHandler("order", orderHandler);
          }
        };

    String balanceChannel = "order";
    String[] channels = {balanceChannel};
    LOG.info("Subscribing to channel : {}", balanceChannel);
    this.service.subscribeToChannels(channels);
    return obs;
  }

  /**
   * Creates a UserTrade from a BittrexOrder
   *
   * @param bittrexOrder
   * @return
   */
  private UserTrade bittrexOrderToUserTrade(BittrexOrder bittrexOrder) {
    return new UserTrade.Builder()
        .type(
            BittrexStreamingUtils.orderDirectionToOrderType(bittrexOrder.getDelta().getDirection()))
        .currencyPair(BittrexUtils.toCurrencyPair(bittrexOrder.getDelta().getMarketSymbol()))
        .orderId(bittrexOrder.getDelta().getId())
        .price(bittrexOrder.getDelta().getLimit())
        .originalAmount(bittrexOrder.getDelta().getQuantity())
        .timestamp(bittrexOrder.getDelta().getCreatedAt())
        .feeAmount(bittrexOrder.getDelta().getCommission())
        .build();
  }
}
