package org.knowm.xchange.bittrex.dto.marketdata;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BittrexLevelV3 {

  private final BigDecimal rate;
  private final BigDecimal quantity;

  /**
   * Constructor
   *
   * @param rate
   * @param quantity
   */
  public BittrexLevelV3(
      @JsonProperty("rate") BigDecimal rate, @JsonProperty("quantity") BigDecimal quantity) {

    this.rate = rate;
    this.quantity = quantity;
  }

  public BigDecimal getPrice() {

    return rate;
  }

  public BigDecimal getAmount() {

    return quantity;
  }

  @Override
  public String toString() {

    return "BittrexLevel [rate=" + rate + ", quantity=" + quantity + "]";
  }
}
