package info.bitrich.xchangestream.bittrex.services.account;

import info.bitrich.xchangestream.bittrex.services.BittrexStreamingService;
import info.bitrich.xchangestream.core.StreamingAccountService;
import io.reactivex.Observable;
import org.knowm.xchange.bittrex.service.BittrexAccountService;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.Balance;

/** See https://bittrex.github.io/api/v3#topic-Websocket-Overview */
public class BittrexStreamingAccountService implements StreamingAccountService {

  private final BittrexStreamingBalancesService bittrexStreamingBalancesService;

  public BittrexStreamingAccountService(
      BittrexStreamingService bittrexStreamingService,
      BittrexAccountService bittrexAccountService) {
    this.bittrexStreamingBalancesService =
        new BittrexStreamingBalancesService(bittrexStreamingService, bittrexAccountService);
  }

  @Override
  public Observable<Balance> getBalanceChanges(Currency currency, Object... args) {
    return bittrexStreamingBalancesService.getBalanceChanges(currency);
  }
}
