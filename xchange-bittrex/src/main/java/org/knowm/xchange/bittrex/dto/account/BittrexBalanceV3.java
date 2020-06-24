package org.knowm.xchange.bittrex.dto.account;

import java.math.BigDecimal;
import java.util.Date;

import org.knowm.xchange.currency.Currency;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class BittrexBalanceV3 {
  private Currency currencySymbol;
  private BigDecimal total;
  private BigDecimal available;
  private Date updatedAt;
}
