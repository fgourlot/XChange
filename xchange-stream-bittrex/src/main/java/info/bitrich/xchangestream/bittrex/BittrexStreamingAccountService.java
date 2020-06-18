package info.bitrich.xchangestream.bittrex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.signalr4j.client.hubs.SubscriptionHandler1;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalance;
import info.bitrich.xchangestream.bittrex.dto.BittrexBalanceDelta;
import info.bitrich.xchangestream.core.StreamingAccountService;
import io.reactivex.Observable;
import io.reactivex.Observer;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;

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

        String balanceChannel = "balance";
        String[] channels = {balanceChannel};
        LOG.info("Subscribing to channel : {}", balanceChannel);

        Observable<Balance> obs =
                new Observable<>() {
                    @Override
                    protected void subscribeActual(Observer<? super Balance> observer) {
                        SubscriptionHandler1 balanceHandler =
                                (SubscriptionHandler1<String>)
                                        message -> {
                                            LOG.debug("Incoming balance message : {}", message);
                                            try {
                                                String decommpressedMessage = EncryptionUtility.decompress(message);
                                                LOG.debug("Decompressed balance message : {}", decommpressedMessage);
                                                // parse JSON to Object
                                                BittrexBalance bittrexBalance =
                                                        objectMapper.readValue(decommpressedMessage, BittrexBalance.class);
                                                // forge OrderBook from BittrexOrderBook
                                                Balance balance = bittrexBalanceToBalance(bittrexBalance);
                                                observer.onNext(balance);
                                            } catch (IOException e) {
                                                e.printStackTrace();
                                            }
                                        };
                        service.setHandler("balance", balanceHandler);
                    }
                };


        this.service.subscribeToChannels(channels);

        return obs;
    }

    private Balance bittrexBalanceToBalance(BittrexBalance bittrexBalance) {
        BittrexBalanceDelta balanceDelta = bittrexBalance.getDelta();
        Balance balance = new Balance(balanceDelta.getCurrencySymbol(), BigDecimal.valueOf(balanceDelta.getTotal()), BigDecimal.valueOf(balanceDelta.getAvailable()));
        return balance;
    }
}
