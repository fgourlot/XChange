package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.core.StreamingAccountService;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BittrexStreamingAccountService implements StreamingAccountService {

  private static final Logger LOG = LoggerFactory.getLogger(BittrexStreamingAccountService.class);

  private final BittrexStreamingService service;

  ObjectMapper objectMapper;

  public BittrexStreamingAccountService(BittrexStreamingService service) {
    this.service = service;
    objectMapper = new ObjectMapper();
  }

  @Override
  public Observable<Balance> getBalanceChanges(Currency currency, Object... args) {

    // create result Observable
    Observable<Balance> obs =
        new Observable<>() {
          @Override
          protected void subscribeActual(Observer<? super Balance> observer) {
            // create handler for `balance` messages
            SubscriptionHandler1<String> balanceHandler =
                message -> {
                  LOG.debug("Incoming balance message : {}", message);
                  try {
                    String decompressedMessage = EncryptionUtility.decompress(message);
                    LOG.debug("Decompressed balance message : {}", decompressedMessage);
                    // parse JSON to Object
                    BittrexBalance bittrexBalance =
                        objectMapper.readValue(decompressedMessage, BittrexBalance.class);
                    Balance balance =
                        new Balance.Builder()
                        .currency(bittrexBalance.getDelta().getCurrencySymbol())
                        .total(bittrexBalance.getDelta().getTotal())
                        .timestamp(bittrexBalance.getDelta().getUpdatedAt())
                        .build();
                    LOG.debug(
                        "Emitting Balance on currency {} with {} available on {} total",
                        balance.getCurrency(),
                        balance.getAvailable(),
                        balance.getTotal());
                    observer.onNext(balance);

                  } catch (IOException e) {
                    LOG.error("Error while receiving and treating balance message", e);
                    throw new RuntimeException(e);
                  }
                };
            service.setHandler("balance", balanceHandler);
          }
        };

    String balanceChannel = "balance";
    String[] channels = {balanceChannel};
    LOG.info("Subscribing to channel : {}", balanceChannel);
    this.service.subscribeToChannels(channels);

    return obs;
  }
}
