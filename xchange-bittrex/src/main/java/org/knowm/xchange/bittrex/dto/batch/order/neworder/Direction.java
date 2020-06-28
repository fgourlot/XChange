package org.knowm.xchange.bittrex.dto.batch.order.neworder;

import org.knowm.xchange.bittrex.BittrexConstants;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum Direction {
  BUY(BittrexConstants.BUY),
  SELL(BittrexConstants.SELL);

  private final String direction;
}
