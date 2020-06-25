package org.knowm.xchange.bittrex.dto.batch.order.neworder;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Direction {
  BUY("BUY"),
  SELL("SELL");

  private final String direction;
}
