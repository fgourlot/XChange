package org.knowm.xchange.bittrex.service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.dto.account.BittrexAccountVolume;
import org.knowm.xchange.bittrex.dto.account.BittrexBalance;
import org.knowm.xchange.bittrex.dto.account.BittrexBalanceV3;
import org.knowm.xchange.bittrex.dto.account.BittrexDepositHistory;
import org.knowm.xchange.bittrex.dto.account.BittrexWithdrawalHistory;
import org.knowm.xchange.bittrex.dto.trade.BittrexOrder;
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

    return bittrexAuthenticatedV3.getBalances(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3);
  }

  public BittrexBalanceV3 getBittrexBalance(Currency currency) throws IOException {
    return bittrexAuthenticatedV3
        .getBalance(
            apiKey,
            System.currentTimeMillis(),
            contentCreator,
            signatureCreatorV3,
            currency.getCurrencyCode());
  }

  public BittrexOrder getBittrexOrder(String uuid) throws IOException {
    return bittrexAuthenticated
        .getOrder(apiKey, signatureCreator, exchange.getNonceFactory(), uuid)
        .getResult();
  }

  public String getBittrexDepositAddress(String currency) throws IOException {

    return bittrexAuthenticated
        .getdepositaddress(apiKey, signatureCreator, exchange.getNonceFactory(), currency)
        .getResult()
        .getAddress();
  }

  public List<BittrexWithdrawalHistory> getWithdrawalsHistory(Currency currency)
      throws IOException {

    return bittrexAuthenticated
        .getwithdrawalhistory(
            apiKey,
            signatureCreator,
            exchange.getNonceFactory(),
            currency == null ? null : currency.getCurrencyCode())
        .getResult();
  }

  public List<BittrexDepositHistory> getDepositsHistory(Currency currency) throws IOException {

    return bittrexAuthenticated
        .getdeposithistory(
            apiKey,
            signatureCreator,
            exchange.getNonceFactory(),
            currency == null ? null : currency.getCurrencyCode())
        .getResult();
  }

  public String withdraw(String currencyCode, BigDecimal amount, String address, String paymentId)
      throws IOException {

    return bittrexAuthenticated
        .withdraw(
            apiKey,
            signatureCreator,
            exchange.getNonceFactory(),
            currencyCode,
            amount.toPlainString(),
            address,
            paymentId)
        .getResult()
        .getUuid();
  }

  public BittrexAccountVolume getAccountVolume() throws IOException {
    return bittrexAuthenticatedV3.getAccountVolume(
        apiKey, System.currentTimeMillis(), contentCreator, signatureCreatorV3);
  }
}
