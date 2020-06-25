package org.knowm.xchange.bittrex.service;

import java.io.IOException;
import java.util.Collection;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.dto.account.BittrexAccountVolume;
import org.knowm.xchange.bittrex.dto.account.BittrexBalanceV3;
import org.knowm.xchange.bittrex.dto.trade.BittrexOrderV3;
import org.knowm.xchange.currency.Currency;

public class BittrexAccountServiceRaw extends BittrexBaseService {

  /**
   * Constructor
   *
   * @param exchange
   */
  public BittrexAccountServiceRaw(Exchange exchange) {

    super(exchange);
  }

  public Collection<BittrexBalanceV3> getBittrexBalances() throws IOException {

    return bittrexAuthenticated.getBalances(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreator);
  }

  public BittrexBalanceV3 getBittrexBalance(Currency currency) throws IOException {
    return bittrexAuthenticated
        .getBalance(
            apiKey,
            System.currentTimeMillis(),
            contentCreator,
            signatureCreator,
            currency.getCurrencyCode());
  }

  public BittrexOrderV3 getBittrexOrder(String orderId) throws IOException {
    return bittrexAuthenticated
        .getOrder(apiKey,
                  System.currentTimeMillis(),
                  contentCreator,
                  signatureCreator,
                  orderId);
  }

  public BittrexAccountVolume getAccountVolume() throws IOException {
    return bittrexAuthenticated.getAccountVolume(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreator);
  }
}
