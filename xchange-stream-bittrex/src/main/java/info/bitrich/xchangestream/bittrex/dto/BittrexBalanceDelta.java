package info.bitrich.xchangestream.bittrex.dto;

import org.knowm.xchange.currency.Currency;

import java.math.BigDecimal;
import java.util.Date;

public class BittrexBalanceDelta {
  private Currency currencySymbol;
  private BigDecimal total;
  private BigDecimal available;
  private Date updatedAt;

  public BittrexBalanceDelta() {}

  public BittrexBalanceDelta(
          Currency currencySymbol, BigDecimal total, BigDecimal available, Date updatedAt) {
    this.currencySymbol = currencySymbol;
    this.total = total;
    this.available = available;
    this.updatedAt = updatedAt;
  }

  public Currency getCurrencySymbol() {
    return currencySymbol;
  }

  public BigDecimal getTotal() {
    return total;
  }

  public BigDecimal getAvailable() {
    return available;
  }

  public Date getUpdatedAt() {
    return updatedAt;
  }
}
