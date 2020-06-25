package org.knowm.xchange.bittrex.service;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.knowm.xchange.dto.account.FundingRecord.Type.DEPOSIT;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.bittrex.BittrexExchange;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.dto.account.AccountInfo;
import org.knowm.xchange.dto.account.Balance;
import org.knowm.xchange.dto.account.FundingRecord;
import org.knowm.xchange.dto.account.Wallet;
import org.knowm.xchange.service.trade.params.TradeHistoryParams;

/** @author walec51 */
public class AccountMockedIntegrationTest extends BaseMockedIntegrationTest {

  private static BittrexAccountService accountService;

  @Before
  public void setUp() {
    accountService = (BittrexAccountService) createExchange().getAccountService();
  }

  @Test
  public void accountInfoTest() throws Exception {
    stubFor(
        get(urlPathEqualTo("/api/v3/balances"))
            .willReturn(
                aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBodyFile("balances.json")));
    AccountInfo accountInfo = accountService.getAccountInfo();
    assertThat(accountInfo).isNotNull();

    Wallet wallet = accountInfo.getWallet();
    assertThat(wallet).isNotNull();

    Balance btcBalance = wallet.getBalance(Currency.BTC);

    // What's in the mocked json
    Currency expectedCurrency = Currency.BTC;
    BigDecimal expectedTotal = new BigDecimal("5265.89272032");
    BigDecimal expectedAvailable = new BigDecimal("626.54401024");
    BigDecimal expectedFrozen = expectedTotal.subtract(expectedAvailable);
    Date expectedTimestamp = Date.from(ZonedDateTime.parse("2020-06-25T14:38:46.06Z").toInstant());
    int expectedNumberOfBalances = 3;

    assertThat(wallet.getBalances().size()).isEqualTo(expectedNumberOfBalances);
    assertThat(btcBalance.getCurrency()).isEqualTo(expectedCurrency);
    assertThat(btcBalance.getTotal().compareTo(expectedTotal)).isEqualTo(0);
    assertThat(btcBalance.getAvailable().compareTo(expectedAvailable)).isEqualTo(0);
    assertThat(btcBalance.getFrozen().compareTo(expectedFrozen)).isEqualTo(0);
    assertThat(btcBalance.getTimestamp()).isEqualTo(expectedTimestamp);
  }
}
