package info.bitrich.xchangestream.bittrex.dto;

import java.math.BigDecimal;

public class BittrexOrderBookEntry {
  private BigDecimal quantity;
  private BigDecimal rate;

  public BittrexOrderBookEntry() {}

  public BittrexOrderBookEntry(BigDecimal quantity, BigDecimal rate) {
    this.quantity = quantity;
    this.rate = rate;
  }

  public BigDecimal getQuantity() {
    return quantity;
  }

  public BigDecimal getRate() {
    return rate;
  }
}
