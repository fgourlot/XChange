package org.knowm.xchange.bittrex.dto.marketdata;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class BittrexTickerV3 {
  private String symbol;
  private BigDecimal lastTradeRate;
  private BigDecimal bidRate;
  private BigDecimal askRate;
}
